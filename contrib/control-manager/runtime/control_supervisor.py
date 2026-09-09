#!/usr/bin/env python3
"""Independent, observation-first recovery supervisor for Control Manager.

It is deliberately outside the broker/Herdr process trees.  Its Unix socket is
owner-only and its SQLite journal survives a supervisor crash.  The default
mode is observation-only; an explicitly enabled guarded-live deployment may
use the concrete adapters below after its rollout gates.  The persistent
3/15-minute limit is enforced for every claimed action.
"""
from __future__ import annotations
import argparse, asyncio, contextlib, fcntl, json, os, signal, socket, sqlite3, stat, struct, time, uuid
from pathlib import Path
from typing import Any

from control_portable import runtime_root

INTERVAL, PROBE_TIMEOUT, FAILURE_THRESHOLD = 10.0, 5.0, 3
CLAIM_LEASE_SECONDS = 120.0
RESTART_LIMIT, RESTART_WINDOW, COOLDOWN = 3, 900.0, 900.0
REQUIRED_TOOLS = {"completion_inbox", "ack_completion", "dispatch", "run_command", "continue_task", "status", "reply", "request_stop"}

def utc() -> float: return time.time()

def systemd_notify(message: str) -> None:
    """Best-effort watchdog tick; absence of systemd is normal in tests."""
    target = os.environ.get("NOTIFY_SOCKET")
    if not target: return
    if target.startswith("@"): target = "\0" + target[1:]
    try:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        client.connect(target); client.sendall(message.encode()); client.close()
    except OSError: pass

class RecoveryDB:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True); self.conn = sqlite3.connect(path); self.conn.row_factory = sqlite3.Row
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS recovery_observations(component TEXT PRIMARY KEY,status TEXT NOT NULL,reason TEXT NOT NULL,failures INTEGER NOT NULL DEFAULT 0,checked_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS recovery_actions(action_id TEXT PRIMARY KEY,component TEXT NOT NULL,kind TEXT NOT NULL,state TEXT NOT NULL,claim_key TEXT NOT NULL,reason TEXT NOT NULL,created_at REAL NOT NULL,updated_at REAL NOT NULL,outcome_json TEXT,owner_generation TEXT NOT NULL DEFAULT '',lease_expires_at REAL);
        CREATE TABLE IF NOT EXISTS recovery_attempts(component TEXT NOT NULL,at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS recovery_restart_attempts(component TEXT NOT NULL,action_id TEXT NOT NULL,at REAL NOT NULL,UNIQUE(component,action_id));
        CREATE TABLE IF NOT EXISTS recovery_settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS recovery_idempotency(idempotency_key TEXT PRIMARY KEY,request_sha256 TEXT NOT NULL,action_id TEXT NOT NULL REFERENCES recovery_actions(action_id),created_at REAL NOT NULL);
        """)
        columns={row["name"] for row in self.conn.execute("PRAGMA table_info(recovery_actions)")}
        if "owner_generation" not in columns:
            self.conn.execute("ALTER TABLE recovery_actions ADD COLUMN owner_generation TEXT NOT NULL DEFAULT ''")
        if "lease_expires_at" not in columns:
            self.conn.execute("ALTER TABLE recovery_actions ADD COLUMN lease_expires_at REAL")
        self.owner_generation=uuid.uuid4().hex
        self.conn.execute("INSERT OR REPLACE INTO recovery_settings VALUES('owner_generation',?)",(self.owner_generation,))
        self.conn.commit(); os.chmod(path, 0o600)
    def paused(self) -> bool: return self.conn.execute("SELECT value FROM recovery_settings WHERE key='paused'").fetchone() is not None
    def set_paused(self, paused: bool) -> None:
        if paused: self.conn.execute("INSERT OR REPLACE INTO recovery_settings VALUES('paused','1')")
        else: self.conn.execute("DELETE FROM recovery_settings WHERE key='paused'")
        self.conn.commit()
    def observe(self, component: str, ok: bool, reason: str) -> dict[str, Any]:
        old = self.conn.execute("SELECT failures FROM recovery_observations WHERE component=?", (component,)).fetchone()
        failures = 0 if ok else int(old[0] if old else 0) + 1
        status = "healthy" if ok else ("degraded" if failures < FAILURE_THRESHOLD else "unavailable")
        self.conn.execute("INSERT INTO recovery_observations VALUES(?,?,?,?,?) ON CONFLICT(component) DO UPDATE SET status=excluded.status,reason=excluded.reason,failures=excluded.failures,checked_at=excluded.checked_at", (component,status,reason[:500],failures,utc())); self.conn.commit()
        return {"component": component,"status": status,"reason": reason[:500],"failures": failures}
    def observe_state(self, component: str, status: str, reason: str) -> dict[str, Any]:
        if status not in {"healthy","degraded","recovering","waiting_user","unavailable"}: raise ValueError("invalid component status")
        failures=0 if status == "healthy" else 1
        self.conn.execute("INSERT INTO recovery_observations VALUES(?,?,?,?,?) ON CONFLICT(component) DO UPDATE SET status=excluded.status,reason=excluded.reason,failures=excluded.failures,checked_at=excluded.checked_at",(component,status,reason[:500],failures,utc())); self.conn.commit()
        return {"component":component,"status":status,"reason":reason[:500],"failures":failures}
    def claim(self, component: str, kind: str, reason: str, *, idempotency_key: str | None = None, admission_budget: bool = True) -> dict[str, Any]:
        """Atomically admit one action across supervisor processes/restarts."""
        request = json.dumps({"component":component,"kind":kind,"reason":reason},sort_keys=True,separators=(",",":"))
        digest = __import__("hashlib").sha256(request.encode()).hexdigest()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            if idempotency_key:
                prior=self.conn.execute("SELECT i.request_sha256,a.* FROM recovery_idempotency i JOIN recovery_actions a ON a.action_id=i.action_id WHERE i.idempotency_key=?",(idempotency_key,)).fetchone()
                if prior:
                    if prior["request_sha256"] != digest: raise ValueError("recovery idempotency key was reused for a different action")
                    self.conn.commit(); return dict(prior) | {"admitted":False,"idempotency_replayed":True}
            key = f"{component}:{kind}"
            # Recovery can touch interconnected shared components.  One
            # durable supervisor owns one active recovery globally, not one
            # action per component/kind.
            # An owner that stopped renewing its lease left an ambiguous
            # external effect. Preserve that evidence and never let a new
            # action silently assume it did not happen.
            self.conn.execute("UPDATE recovery_actions SET state='uncertain',updated_at=?,outcome_json=? WHERE state IN ('claimed','recovering') AND lease_expires_at IS NOT NULL AND lease_expires_at<=?",
                              (utc(),json.dumps({"state":"uncertain","reason":"recovery owner lease expired; action was not replayed"},sort_keys=True),utc()))
            row = self.conn.execute("SELECT * FROM recovery_actions WHERE state IN ('claimed','recovering') ORDER BY created_at LIMIT 1").fetchone()
            if row:
                self.conn.commit(); return dict(row) | {"admitted": False}
        # A double-click or client retry immediately after a completed probe
        # is also one recovery request.  It must not consume another attempt.
            row = self.conn.execute("SELECT * FROM recovery_actions WHERE component=? AND kind=? AND updated_at>? ORDER BY updated_at DESC LIMIT 1", (component,kind,utc()-INTERVAL)).fetchone()
            if row:
                self.conn.commit(); return dict(row) | {"admitted": False, "idempotency_replayed": True}
            now=utc()
            cooldown_key=f"cooldown:{component}"
            saved=self.conn.execute("SELECT value FROM recovery_settings WHERE key=?",(cooldown_key,)).fetchone()
            if saved and admission_budget:
                try: until=float(saved[0])
                except (TypeError,ValueError): until=0
                if until > now:
                    self.conn.commit(); raise RuntimeError(f"{component} is in {int(COOLDOWN/60)}-minute cooldown until {int(until)}")
                self.conn.execute("DELETE FROM recovery_settings WHERE key=?",(cooldown_key,))
            recent = self.conn.execute("SELECT count(*) FROM recovery_attempts WHERE component=? AND at>?", (component,now-RESTART_WINDOW)).fetchone()[0]
            if admission_budget and recent >= RESTART_LIMIT:
                until=now+COOLDOWN
                # The new cooldown is persisted exactly once. Later rejected
                # requests read it; they never extend an outage indefinitely.
                self.conn.execute("INSERT INTO recovery_settings(key,value) VALUES(?,?) ON CONFLICT(key) DO NOTHING",(cooldown_key,str(until)))
                self.conn.commit(); raise RuntimeError(f"{component} is in {int(COOLDOWN/60)}-minute cooldown until {int(until)}")
            ident = f"recovery-{uuid.uuid4().hex}"
            # This is a capability, not an identifier convention.  The broker
            # independently checks it against this owner-only journal before
            # it will replace a lost manager terminal binding.
            key = uuid.uuid4().hex + uuid.uuid4().hex
            self.conn.execute("INSERT INTO recovery_actions(action_id,component,kind,state,claim_key,reason,created_at,updated_at,outcome_json,owner_generation,lease_expires_at) VALUES(?,?,?,?,?,?,?,?,NULL,?,?)", (ident,component,kind,"claimed",key,reason[:500],now,now,self.owner_generation,now+CLAIM_LEASE_SECONDS))
            self.conn.execute("INSERT INTO recovery_attempts VALUES(?,?)",(component,now))
            if idempotency_key: self.conn.execute("INSERT INTO recovery_idempotency VALUES(?,?,?,?)",(idempotency_key,digest,ident,now))
            self.conn.commit()
            return {"action_id": ident,"claim_key":key,"owner_generation":self.owner_generation,"lease_expires_at":now+CLAIM_LEASE_SECONDS,"component":component,"kind":kind,"state":"claimed","admitted":True}
        except Exception:
            self.conn.rollback()
            raise
    def finish(self, action: dict[str,Any], outcome: dict[str,Any], state: str = "completed") -> None:
        if state not in {"completed","failed","blocked","uncertain"}: raise ValueError("invalid recovery outcome state")
        self.conn.execute("UPDATE recovery_actions SET state=?,updated_at=?,outcome_json=? WHERE action_id=?",(state,utc(),json.dumps(outcome,sort_keys=True),action["action_id"])); self.conn.commit()
    def refresh_claim(self, action: dict[str,Any]) -> dict[str,Any]:
        """Fence each external action against pause, expiry and owner takeover."""
        now=utc(); ident=str(action.get("action_id") or ""); key=str(action.get("claim_key") or "")
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            if self.paused(): raise RuntimeError("automatic recovery is paused by the deployment owner")
            row=self.conn.execute("SELECT * FROM recovery_actions WHERE action_id=?",(ident,)).fetchone()
            if (row is None or row["state"] not in {"claimed","recovering"}
                    or row["owner_generation"] != self.owner_generation or row["claim_key"] != key
                    or row["lease_expires_at"] is None or float(row["lease_expires_at"]) <= now):
                raise RuntimeError("recovery action is no longer owned by this supervisor generation")
            expiry=now+CLAIM_LEASE_SECONDS
            self.conn.execute("UPDATE recovery_actions SET state='recovering',updated_at=?,lease_expires_at=? WHERE action_id=?",(now,expiry,ident))
            self.conn.commit()
            return {"owner_generation":self.owner_generation,"lease_expires_at":expiry}
        except Exception:
            if self.conn.in_transaction: self.conn.rollback()
            raise
    def reserve_restart(self, component: str, action_id: str) -> None:
        """Atomically enforce the restart budget for the component being touched.

        Admission budgets remain for compatibility with older recovery callers;
        this distinct ledger is what prevents an ``all`` action from hiding
        several component restarts behind one aggregate attempt.
        """
        now=utc(); key=f"restart_cooldown:{component}"
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            if self.conn.execute("SELECT 1 FROM recovery_restart_attempts WHERE component=? AND action_id=?",(component,action_id)).fetchone():
                self.conn.commit(); return
            saved=self.conn.execute("SELECT value FROM recovery_settings WHERE key=?",(key,)).fetchone()
            until=float(saved[0]) if saved else 0
            if until > now:
                self.conn.commit(); raise RuntimeError(f"{component} restart is in {int(COOLDOWN/60)}-minute cooldown until {int(until)}")
            self.conn.execute("DELETE FROM recovery_settings WHERE key=?",(key,))
            recent=self.conn.execute("SELECT count(*) FROM recovery_restart_attempts WHERE component=? AND at>?",(component,now-RESTART_WINDOW)).fetchone()[0]
            if recent >= RESTART_LIMIT:
                until=now+COOLDOWN
                self.conn.execute("INSERT OR REPLACE INTO recovery_settings VALUES(?,?)",(key,str(until)))
                self.conn.commit(); raise RuntimeError(f"{component} restart is in {int(COOLDOWN/60)}-minute cooldown until {int(until)}")
            self.conn.execute("INSERT INTO recovery_restart_attempts VALUES(?,?,?)",(component,action_id,now)); self.conn.commit()
        except Exception:
            if self.conn.in_transaction: self.conn.rollback()
            raise
    def reconcile_interrupted_actions(self) -> int:
        """Fail closed after a crash: record uncertainty and never replay."""
        rows = self.conn.execute("SELECT action_id FROM recovery_actions WHERE state IN ('claimed','recovering')").fetchall()
        now = utc()
        for row in rows:
            outcome = {"state": "uncertain", "reason": "supervisor restarted before durable recovery outcome; action was not replayed"}
            self.conn.execute("UPDATE recovery_actions SET state='uncertain',updated_at=?,outcome_json=? WHERE action_id=?", (now, json.dumps(outcome, sort_keys=True), row["action_id"]))
        self.conn.commit()
        return len(rows)
    def overall_status(self) -> str:
        if self.paused(): return "waiting_user"
        states = {row["status"] for row in self.conn.execute("SELECT status FROM recovery_observations")}
        if "waiting_user" in states: return "waiting_user"
        if "unavailable" in states: return "unavailable"
        if "degraded" in states: return "degraded"
        if "recovering" in states: return "recovering"
        return "healthy" if states else "recovering"
    def status(self, affected_sessions: list[str] | None = None) -> dict[str,Any]:
        components=[dict(x) for x in self.conn.execute("SELECT * FROM recovery_observations ORDER BY component")]
        # Claim capabilities authorize a broker-side terminal replacement;
        # status observers need action state, never bearer capabilities.
        history=[{key:value for key,value in dict(x).items() if key != "claim_key"}
                 for x in self.conn.execute("SELECT * FROM recovery_actions ORDER BY updated_at DESC LIMIT 20")]
        active=[item for item in history if item["state"] in {"claimed","recovering"}]
        health={item["component"]:{"state":item["status"],"reason":item["reason"],"checked_at":item["checked_at"]} for item in components}
        return {"state":self.overall_status(),"paused":self.paused(),"components":components,"health":health,"recent_outcomes":history,"history":history,"active_actions":active,"affected_sessions":affected_sessions or []}

async def unix_probe(path: str, payload: dict[str, Any] | None = None) -> tuple[bool,str]:
    """Bound a complete control-plane round trip, not just connect()."""
    if not path: return False,"not configured"
    writer: asyncio.StreamWriter | None = None
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_unix_connection(path), PROBE_TIMEOUT)
        if payload is not None:
            writer.write(json.dumps(payload, separators=(",", ":")).encode() + b"\n")
            await asyncio.wait_for(writer.drain(), PROBE_TIMEOUT)
            raw = await asyncio.wait_for(reader.readline(), PROBE_TIMEOUT)
            if not raw:
                raise RuntimeError("peer closed without a probe response")
            response = json.loads(raw)
            if response.get("error") or response.get("ok") is False:
                raise RuntimeError(f"probe rejected: {response.get('error', response)}")
        return True,"reachable and responsive"
    except Exception as exc: return False,f"{type(exc).__name__}: {exc}"
    finally:
        if writer is not None:
            writer.close()
            with contextlib.suppress(Exception): await writer.wait_closed()

async def bounded_process(argv: list[str], env: dict[str, str], timeout: float = PROBE_TIMEOUT) -> tuple[int, str, str]:
    """Run a configured isolated recovery helper with bounded, drained output."""
    process = await asyncio.create_subprocess_exec(*argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env, start_new_session=True)
    async def drain(reader: asyncio.StreamReader | None) -> str:
        if reader is None: return ""
        kept=bytearray()
        while chunk := await reader.read(4096):
            if len(kept) < 8192: kept.extend(chunk[:8192-len(kept)])
        return kept.decode(errors="replace")
    out_task=asyncio.create_task(drain(process.stdout)); err_task=asyncio.create_task(drain(process.stderr))
    async def stop_process_group() -> None:
        """Reap every descendant on both timeout and caller cancellation."""
        # Descendants may retain output pipes after the helper leader exits.
        with contextlib.suppress(ProcessLookupError): os.killpg(process.pid,signal.SIGTERM)
        if process.returncode is None:
            with contextlib.suppress(asyncio.TimeoutError): await asyncio.wait_for(process.wait(), 2)
        with contextlib.suppress(ProcessLookupError): os.killpg(process.pid,signal.SIGKILL)
        if process.returncode is None:
            with contextlib.suppress(ProcessLookupError): await process.wait()
    async def complete() -> tuple[str,str]:
        await process.wait()
        return tuple(await asyncio.gather(out_task,err_task))
    try:
        output,error=await asyncio.wait_for(complete(), timeout)
    except asyncio.TimeoutError:
        await stop_process_group()
        for task in (out_task,err_task): task.cancel()
        await asyncio.gather(out_task,err_task,return_exceptions=True)
        raise RuntimeError("TimeoutError: recovery helper exceeded bounded deadline")
    except asyncio.CancelledError:
        # A disconnected or timed-out caller must not leak a helper which can
        # later perform an unobserved consequential action.
        await stop_process_group()
        for task in (out_task,err_task): task.cancel()
        await asyncio.gather(out_task,err_task,return_exceptions=True)
        raise
    return process.returncode or 0,output,error

async def appserver_probe(cfg: dict[str, Any], config_path: Path) -> tuple[bool, str, dict[str, Any]]:
    """Authoritatively read the configured manager thread and actual tools."""
    thread=str(cfg.get("manager_thread_id") or "")
    if not cfg.get("app_server_socket") or not thread: return False,"app-server socket or canonical manager thread is not configured",{}
    command=cfg.get("appserver_probe_command") or ["/usr/bin/python3",str(runtime_root() / "appserver_manager.py"),"probe",thread]
    if not isinstance(command,list) or not all(isinstance(item,str) for item in command): return False,"invalid appserver_probe_command",{}
    env=os.environ | {"CODEX_APP_SERVER_SOCKET":str(cfg["app_server_socket"]),"CODEX_CONTROL_CONFIG_PATH":str(config_path)}
    try:
        code,out,err=await bounded_process(command,env)
        if code: return False,f"app-server probe exit {code}: {(err or out)[-400:]}",{}
        value=json.loads(out)
        actual=(value.get("thread") or {})
        if actual.get("id") != thread: return False,"app-server returned a different manager thread",value
        # A responsive, exact thread is app-server readiness. MCP capability is
        # a separate component: a tool gap must never become evidence that the
        # app-server itself is down or license a restart/continuation.
        return True,"canonical thread and turn read successfully",value
    except Exception as exc: return False,f"{type(exc).__name__}: {exc}",{}

async def native_manager_probe(cfg: dict[str, Any], config_path: Path) -> tuple[str, str, dict[str, Any]]:
    """Bound a read-only check of the reserved native manager pane/session."""
    thread=str(cfg.get("manager_thread_id") or "")
    if not thread:
        return "degraded","canonical manager thread is not configured",{}
    if not cfg.get("manager_pane_id") or not cfg.get("manager_workspace_id"):
        # Do not invent a pane binding.  This is fail-closed readiness (not an
        # automatic recovery target) until rollout has supplied both ids.
        return "degraded","configured native manager pane binding is incomplete",{}
    command=cfg.get("native_manager_probe_command") or ["/usr/bin/python3",str(runtime_root() / "appserver_manager.py"),"native-pane-health",thread]
    if not isinstance(command,list) or not all(isinstance(item,str) for item in command):
        return "degraded","invalid native_manager_probe_command",{}
    env=os.environ | {"CODEX_CONTROL_CONFIG_PATH":str(config_path)}
    if cfg.get("herdr_socket"): env["CODEX_CONTROL_HERDR_SOCKET"]=str(cfg["herdr_socket"])
    if cfg.get("app_server_socket"): env["CODEX_APP_SERVER_SOCKET"]=str(cfg["app_server_socket"])
    try:
        code,out,err=await bounded_process(command,env)
        if code: return "unavailable",f"native manager pane probe exit {code}: {(err or out)[-400:]}",{}
        value=json.loads(out)
        state=str(value.get("state") or "degraded")
        if state not in {"healthy","unavailable","degraded","waiting_user"}:
            return "degraded","native manager pane probe returned an invalid state",value
        return state,str(value.get("reason") or "native manager pane probe returned no reason")[:500],value
    except Exception as exc:
        return "unavailable",f"{type(exc).__name__}: {exc}",{}

async def broker_reconcile(socket_path: str, *, topology: bool = False, action_id: str | None = None,
                           claim_key: str | None = None, owner_generation: str | None = None) -> tuple[bool,str]:
    """Ask the broker to reconcile durable work through its owner socket."""
    writer: asyncio.StreamWriter | None=None
    try:
        reader,writer=await asyncio.wait_for(asyncio.open_unix_connection(socket_path),PROBE_TIMEOUT)
        if topology:
            if not action_id or not claim_key or not owner_generation:
                return False,"topology reconciliation requires its durable supervisor claim capability"
            payload={"method":"reconcile_topology","params":{
                "recovery_action_id":action_id, "recovery_claim_key":claim_key,
                "recovery_owner_generation":owner_generation,
            }}
        else:
            payload={"method":"reconcile","params":{}}
        writer.write(json.dumps(payload,separators=(",", ":")).encode()+b"\n"); await asyncio.wait_for(writer.drain(),PROBE_TIMEOUT)
        raw=await asyncio.wait_for(reader.readline(),PROBE_TIMEOUT)
        response=json.loads(raw)
        if not response.get("ok"): return False,str(response.get("error","broker reconcile rejected"))
        return True,"durable broker reconciliation completed"
    except Exception as exc: return False,f"{type(exc).__name__}: {exc}"
    finally:
        if writer: writer.close();

class Supervisor:
    def __init__(self, socket_path: Path, db_path: Path, config: Path):
        self.socket_path,self.config=socket_path,config
        lock_paths=sorted({Path(str(db_path)+".owner.lock"),Path(str(socket_path)+".lock")},key=str)
        self._lock_fds=[]
        try:
            for lock_path in lock_paths:
                lock_path.parent.mkdir(parents=True,exist_ok=True); fd=os.open(lock_path,os.O_CREAT|os.O_RDWR,0o600)
                try: fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except Exception: os.close(fd); raise
                self._lock_fds.append(fd)
        except BlockingIOError:
            for fd in self._lock_fds: os.close(fd)
            raise RuntimeError(f"recovery supervisor already owns socket or database: {socket_path}, {db_path}")
        self._lock_fd=self._lock_fds[0]
        self.db=RecoveryDB(db_path)
        self.interrupted_actions=self.db.reconcile_interrupted_actions(); self.stop=asyncio.Event()
        self.observer_process: asyncio.subprocess.Process | None = None
        self.observer_drains: list[asyncio.Task[str]] = []
        self.observer_output: dict[str,str] = {"stdout":"","stderr":""}
        self.observer_ready=asyncio.Event(); self.observer_last_heartbeat=0.0
        self.observer_protocol_error=""; self.observer_thread_id=""
        # Every in-flight upstream operation registers a finite deadline.  The
        # heartbeat task refuses to mask an operation that has outlived its
        # contract, while ordinary async waits keep feeding systemd.
        self._work_deadlines: dict[str,float] = {}

    def _begin_work(self, seconds: float) -> str:
        token=uuid.uuid4().hex
        self._work_deadlines[token]=time.monotonic()+max(0.1,seconds)
        return token

    def _end_work(self, token: str) -> None:
        self._work_deadlines.pop(token,None)

    async def _watchdog_heartbeat(self) -> None:
        """Feed systemd independently of bounded upstream I/O.

        If the event loop blocks, this coroutine cannot run.  If an awaited
        checker/action escapes its deadline, it deliberately stops feeding the
        watchdog instead of concealing that defective implementation.
        """
        try: usec=int(os.environ.get("WATCHDOG_USEC","0"))
        except ValueError: usec=0
        try: expected_pid=int(os.environ.get("WATCHDOG_PID",str(os.getpid())))
        except ValueError: return
        if usec <= 0 or expected_pid != os.getpid(): return
        interval=max(0.05,usec/3_000_000)
        while not self.stop.is_set():
            now=time.monotonic()
            if all(now <= deadline for deadline in self._work_deadlines.values()):
                systemd_notify("WATCHDOG=1")
            try: await asyncio.wait_for(self.stop.wait(),interval)
            except asyncio.TimeoutError: pass
    def bindings(self) -> dict[str,Any]:
        try: return json.loads(self.config.read_text())
        except Exception: return {}
    def status(self) -> dict[str,Any]:
        cfg=self.bindings(); sessions=cfg.get("affected_sessions")
        if sessions is None:
            manager=str(cfg.get("manager_thread_name") or cfg.get("manager_thread_id") or "").strip()
            sessions=[manager] if manager else []
        result=self.db.status(sessions if isinstance(sessions,list) and all(isinstance(item,str) for item in sessions) else [])
        mode=str(cfg.get("supervisor_mode","observation_only"))
        live=cfg.get("recovery_live_enabled") is True
        effective=("guarded_live" if mode == "guarded_live" and live else
                   "isolated_active" if mode == "isolated_active" else "observation_only")
        return result | {"supervisor_mode":mode,"recovery_live_enabled":live,"effective_recovery_mode":effective}
    async def _drain_observer(self, name: str, reader: asyncio.StreamReader | None) -> str:
        if reader is None: return ""
        kept=bytearray(); pending=bytearray()
        while chunk := await reader.read(4096):
            if len(kept)<8192: kept.extend(chunk[:8192-len(kept)])
            if name == "stdout":
                pending.extend(chunk)
                if len(pending)>65536:
                    self.observer_protocol_error="observer emitted an oversized unterminated JSON record"; pending.clear()
                while b"\n" in pending:
                    raw,_,rest=pending.partition(b"\n"); pending=bytearray(rest)
                    try: value=json.loads(raw)
                    except (UnicodeDecodeError,json.JSONDecodeError):
                        self.observer_protocol_error="observer emitted invalid JSON"; continue
                    expected=str(self.bindings().get("manager_thread_id") or "")
                    if value.get("ready") is True:
                        if expected and value.get("thread_id") != expected: self.observer_protocol_error="observer ready record named a different manager thread"
                        else: self.observer_thread_id=str(value.get("thread_id") or ""); self.observer_ready.set()
                    heartbeat=value.get("observer_heartbeat")
                    if isinstance(heartbeat,dict):
                        if expected and heartbeat.get("thread_id") != expected: self.observer_protocol_error="observer heartbeat named a different manager thread"
                        elif not isinstance(heartbeat.get("timestamp"),(int,float)): self.observer_protocol_error="observer heartbeat has no numeric timestamp"
                        else: self.observer_last_heartbeat=time.monotonic()
        self.observer_output[name]=kept.decode(errors="replace")
        return self.observer_output[name]
    async def _observer_status(self, cfg: dict[str,Any], *, reconnect: bool=False) -> tuple[bool,str]:
        command=cfg.get("observer_command")
        if not isinstance(command,list) or not command:
            return False,"no managed observer command configured"
        if not all(isinstance(item,str) for item in command):
            return False,"managed observer command arguments must be strings"
        # Resolve the canonical identity from the authoritative config at each
        # launch.  Capability activation may atomically replace the manager
        # thread; retaining a literal here would reconnect the old observer.
        thread=str(cfg.get("manager_thread_id") or "")
        command=[thread if item == "{manager_thread_id}" else item for item in command]
        if "{manager_thread_id}" in cfg.get("observer_command",[]) and not thread:
            return False,"managed observer command requires a canonical manager thread"
        mode=str(cfg.get("supervisor_mode","observation_only"))
        if mode != "isolated_active" and not (mode == "guarded_live" and cfg.get("recovery_live_enabled") is True):
            return False,"managed observer launch is disabled without isolated_active or enabled guarded_live mode"
        if reconnect and self.observer_process and self.observer_process.returncode is None:
            with contextlib.suppress(ProcessLookupError): os.killpg(self.observer_process.pid,signal.SIGTERM)
            with contextlib.suppress(asyncio.TimeoutError): await asyncio.wait_for(self.observer_process.wait(),2)
            if self.observer_process.returncode is None:
                with contextlib.suppress(ProcessLookupError): os.killpg(self.observer_process.pid,signal.SIGKILL)
                await self.observer_process.wait()
        if self.observer_process is None or self.observer_process.returncode is not None:
            try:
                self.observer_ready=asyncio.Event(); self.observer_last_heartbeat=0.0; self.observer_protocol_error=""; self.observer_thread_id=""
                # The observer is a separate process and must not silently
                # fall back to this machine's defaults.  Its active config is
                # authoritative for every endpoint and manager binding.
                env=os.environ.copy()
                bindings={
                    "CODEX_APP_SERVER_SOCKET": cfg.get("app_server_socket"),
                    "CODEX_CONTROL_CONFIG_PATH": str(self.config),
                    "CODEX_CONTROL_HERDR_SOCKET": cfg.get("herdr_socket"),
                    "CONTROL_BROKER_SOCKET": cfg.get("broker_socket"),
                    "CODEX_BINARY": cfg.get("codex_binary"),
                    "CODEX_CONTROL_MANAGER_CWD": cfg.get("manager_cwd"),
                }
                env.update({key:str(value) for key,value in bindings.items() if isinstance(value,str) and value})
                self.observer_process=await asyncio.create_subprocess_exec(*command,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE,env=env,start_new_session=True)
                self.observer_drains=[asyncio.create_task(self._drain_observer("stdout",self.observer_process.stdout)),asyncio.create_task(self._drain_observer("stderr",self.observer_process.stderr))]
            except Exception as exc: return False,f"observer start failed: {type(exc).__name__}: {exc}"
        await asyncio.sleep(0)
        if self.observer_process.returncode is not None:
            return False,f"observer exited {self.observer_process.returncode}: {self.observer_output['stderr'][-300:]}"
        if not self.observer_ready.is_set():
            try: await asyncio.wait_for(self.observer_ready.wait(),PROBE_TIMEOUT)
            except asyncio.TimeoutError: return False,"observer JSON readiness deadline exceeded"
        # Ready and the first heartbeat are consecutive records.  The ready
        # event can wake this coroutine before the drain task parses the next
        # line, so keep the same bounded probe deadline for that first sample.
        heartbeat_deadline=time.monotonic()+PROBE_TIMEOUT
        while not self.observer_last_heartbeat and not self.observer_protocol_error and time.monotonic() < heartbeat_deadline:
            await asyncio.sleep(.01)
        if self.observer_protocol_error: return False,self.observer_protocol_error
        freshness=float(cfg.get("observer_heartbeat_timeout",max(INTERVAL*2+PROBE_TIMEOUT,PROBE_TIMEOUT)))
        if not self.observer_last_heartbeat or time.monotonic()-self.observer_last_heartbeat > freshness:
            return False,"observer heartbeat is missing or stale"
        return True,"managed observer exact-thread readiness and heartbeat verified"
    async def check(self) -> dict[str,Any]:
        # unix probes fan out then the app-server RPC follows; this deadline is
        # intentionally finite even if a future adapter regresses its timeout.
        token=self._begin_work(PROBE_TIMEOUT*2+1)
        try:
            cfg=self.bindings(); results=[]
            basic=(("herdr", "herdr_socket", {"id": "control-supervisor-ping", "method": "ping", "params": {}}),
               ("broker", "broker_socket", {"method": "ping", "params": {}}),
               ("manager_observer", "manager_observer_socket", {"method":"ping","params":{}}),
               ("remote_control", "remote_control_socket", {"method":"ping","params":{}}))
            app_task=asyncio.create_task(appserver_probe(cfg,self.config))
            native_task=asyncio.create_task(native_manager_probe(cfg,self.config))
            observed=await asyncio.gather(*(unix_probe(str(cfg.get(key,"")), payload) for _,key,payload in basic))
            for (name,key,_),(ok,why) in zip(basic,observed):
                if name == "manager_observer" and cfg.get("observer_command") and not cfg.get(key):
                    continue
                if name == "remote_control" and not cfg.get(key):
                    continue
                if not cfg.get(key) and name in {"manager_observer","remote_control"}:
                    results.append(self.db.observe_state(name,"degraded","optional adapter not configured; excluded from automatic recovery"))
                else: results.append(self.db.observe(name,ok,why if cfg.get(key) else "not configured"))
            observer_ok,observer_reason=await self._observer_status(cfg)
            if cfg.get("observer_command"): results.append(self.db.observe("observer",observer_ok,observer_reason))
            app_ok,app_reason,app=await app_task
            native_state,native_reason,native=await native_task
            if not cfg.get("remote_control_socket"):
                remote = app.get("remote_control") or {}
                remote_status = remote.get("status", "unknown")
                results.append(self.db.observe_state("remote_control", "healthy" if remote_status == "connected" else "degraded",
                    f"remote transport: {remote_status}" + (f"; {remote['reason']}" if remote.get("reason") else "")))
            if cfg.get("observer_command") and not cfg.get("manager_observer_socket"):
                # Remove the superseded optional adapter row from an earlier
                # installation; the managed observer has authoritative evidence.
                self.db.conn.execute("DELETE FROM recovery_observations WHERE component = ?", ("manager_observer",))
                self.db.conn.commit()
            results.append(self.db.observe("app_server",app_ok,app_reason))
            thread=app.get("thread") or {}; turn=app.get("turn") or {}; control=app.get("control_broker") or {}
            results.append(self.db.observe("manager_thread",app_ok and bool(thread.get("id")),str(thread.get("status") or app_reason)))
            results.append(self.db.observe_state("manager_native",native_state,native_reason))
        # A silent active turn is not a failure. Only an authoritative terminal
        # error is classified separately from Herdr's activity label.
            terminal=str(turn.get("status") or "") in {"failed","interrupted","cancelled"}
            error=turn.get("error") if isinstance(turn.get("error"),dict) else {}
            info=error.get("codexErrorInfo")
            auth=info == "unauthorized"
            limited=isinstance(info,str) and info in {"usageLimitExceeded","rateLimitExceeded","serverOverloaded"}
            turn_reason=str(turn.get("error") or turn.get("status") or "no turn")
            if app_ok and (auth or turn.get("status") in {"interrupted","cancelled"}):
                results.append(self.db.observe_state("manager_turn","waiting_user",turn_reason))
            elif app_ok and (limited or (turn.get("status")=="failed" and not self._transient(turn.get("error")))):
                results.append(self.db.observe_state("manager_turn","degraded",turn_reason))
            else:
                results.append(self.db.observe("manager_turn",app_ok and not terminal,turn_reason))
            results.append(self.db.observe_state("provider_auth","waiting_user" if auth else "healthy","sign-in required" if auth else "authenticated or no authentication error observed"))
            results.append(self.db.observe_state("provider_health","degraded" if limited else "healthy",f"provider backoff required: {info}" if limited else "no quota/provider backoff condition observed"))
            tools=set(control.get("tools") or [])
            inventory_verified=control.get("inventory_verified") is True
            tools_ok=app_ok and inventory_verified and control.get("runtime_status")=="connected" and REQUIRED_TOOLS <= tools
            tools_reason=("verified" if tools_ok else app_reason if not app_ok else
                      str(control.get("inventory_error") or "MCP inventory was not authoritatively verified") if not inventory_verified else
                      "canonical broker MCP is not connected" if control.get("runtime_status")!="connected" else
                      f"canonical manager required tools missing: {sorted(REQUIRED_TOOLS-tools)}")
            if app_ok and not inventory_verified:
                results.append(self.db.observe_state("broker_tools","degraded",tools_reason))
            else: results.append(self.db.observe("broker_tools",tools_ok,tools_reason))
            return {"state":self.db.overall_status(),"checked_at":utc(),"components":results,"authoritative":{"thread":thread,"turn":turn,"control_broker":control,"manager_native":native},"paused":self.db.paused(),"interrupted_actions_reconciled":self.interrupted_actions}
        finally: self._end_work(token)
    @staticmethod
    def _transient(error: Any) -> bool:
        """Require affirmative installed-protocol transient terminal evidence.

        Current app-server TurnError has ``message``, ``codexErrorInfo`` and
        ``additionalDetails``; it does *not* expose the historical
        ``code``/``retryable`` pair.  Only a terminated/failed response stream
        with no upstream status or an upstream 5xx is positive continuation
        evidence.  Auth, quota, policy, 4xx and unknown provider conditions
        remain degraded/backoff and never select a local continuation/restart.
        """
        if isinstance(error,dict):
            info=error.get("codexErrorInfo")
            if isinstance(info,dict):
                for variant in ("responseStreamDisconnected", "responseStreamConnectionFailed", "httpConnectionFailed"):
                    details=info.get(variant)
                    if not isinstance(details,dict): continue
                    status=details.get("httpStatusCode")
                    # Null means the transport failed before an HTTP response.
                    # A server 5xx is also transport/upstream transient; every
                    # 4xx, notably 401/429, remains non-resumable.
                    return status is None or (isinstance(status,int) and 500 <= status < 600)
                return False
            # Compatibility with durable pre-schema records only. New appserver
            # TurnErrors take the branch above and cannot bypass it.
            allowed={"transport_error","connection_error","model_timeout","temporarily_unavailable","internal_server_error"}
            code=str(error.get("code") or error.get("type") or "").lower()
            return bool(error.get("retryable") is True and code in allowed)
        text=str(error or "").lower()
        return any(token in text for token in ("transient transport", "transient model", "connection reset", "connection refused", "model timeout", "temporarily unavailable", "internal server error"))

    @staticmethod
    def _restartable_infrastructure(reason: str) -> bool:
        lowered=reason.lower()
        return any(token in lowered for token in ("connectionrefused", "connection refused", "filenotfound", "timed out", "timeout", "peer closed", "websocket closed", "socket"))

    async def _run_action(self, cfg: dict[str,Any], action: dict[str,Any], name: str) -> dict[str,Any]:
        token=self._begin_work(float(cfg.get("recovery_action_timeout",30))+2)
        try:
            return await self._run_action_inner(cfg,action,name)
        finally: self._end_work(token)

    async def _run_action_inner(self, cfg: dict[str,Any], action: dict[str,Any], name: str) -> dict[str,Any]:
        mode=str(cfg.get("supervisor_mode", "observation_only"))
        # Fixtures may execute actions in isolated_active.  Any non-fixture
        # action needs both guarded_live mode and the separate live gate; mode
        # alone is never an authorization to touch a shared service/thread.
        allowed = mode == "isolated_active" or (mode == "guarded_live" and cfg.get("recovery_live_enabled") is True)
        if name == "observer_reconnect" and cfg.get("observer_command"):
            ok,reason=await self._observer_status(cfg,reconnect=True)
            return {"state":"completed" if ok else "waiting_rollout" if "observation_only" in reason else "failed","reason":reason,"output":self.observer_output}
        if name == "reconcile_durable_work":
            if not allowed:
                return {"state":"waiting_rollout","reason":"reconciliation requires isolated_active or explicitly enabled guarded_live mode"}
            ok,reason=await broker_reconcile(str(cfg.get("broker_socket") or ""))
            return {"state":"completed" if ok else "failed","reason":reason}
        if name == "reconcile_manager_topology":
            if not allowed:
                return {"state":"waiting_rollout","reason":"topology reconciliation requires isolated_active or explicitly enabled guarded_live mode"}
            # Herdr's socket can accept a ping before its restored workspace
            # and agent inventory are observable.  In that interval the
            # broker quite correctly refuses to claim the canonical pane.
            # Keep the *same* durable recovery action and retry only that
            # exact topology probe: broker-side reattachment is already
            # bounded to the configured thread/pane and is idempotent while a
            # manager launch is in progress.  This avoids turning a startup
            # observation race into a permanently failed infrastructure
            # recovery, without replaying any manager request.
            try: configured_timeout=max(0.0,float(cfg.get("topology_reconciliation_timeout",20)))
            except (TypeError,ValueError): configured_timeout=20.0
            # _run_action's watchdog lease is based on recovery_action_timeout.
            # A topology retry must never outlive that enclosing action lease.
            try: action_timeout=max(0.0,float(cfg.get("recovery_action_timeout",30)))
            except (TypeError,ValueError): action_timeout=30.0
            timeout=min(30.0,configured_timeout,action_timeout)
            deadline=time.monotonic()+timeout
            ok,reason=False,"topology reconciliation deadline exceeded"
            while time.monotonic() < deadline:
                remaining=deadline-time.monotonic()
                try:
                    action.update(self.db.refresh_claim(action))
                    ok,reason=await asyncio.wait_for(broker_reconcile(
                        str(cfg.get("broker_socket") or ""), topology=True,
                        action_id=str(action.get("action_id") or ""),
                        claim_key=str(action.get("claim_key") or ""),
                        owner_generation=str(action.get("owner_generation") or ""),
                    ),remaining)
                except asyncio.TimeoutError:
                    reason="topology reconciliation deadline exceeded; outcome unverified"
                    break
                if ok: break
                await asyncio.sleep(min(.5,max(0.0,deadline-time.monotonic())))
            return {"state":"completed" if ok else "failed","reason":reason,
                    "never_replayed_original_request":True}
        if name == "manager_continuation":
            thread=str(cfg.get("manager_thread_id") or "")
            failed_turn=str(action.get("failed_turn_id") or "")
            if not allowed:
                return {"state":"waiting_rollout","reason":"manager continuation requires isolated_active or explicitly enabled guarded_live mode"}
            if not failed_turn:
                return {"state":"blocked","reason":"exact failed turn id was not retained for authoritative reread"}
            argv=["/usr/bin/python3",str(runtime_root() / "appserver_manager.py"),"resume",thread,failed_turn]
            env=os.environ | {"CODEX_APP_SERVER_SOCKET":str(cfg.get("app_server_socket") or ""),"CODEX_CONTROL_CONFIG_PATH":str(self.config)}
            try:
                code,out,err=await bounded_process(argv,env,timeout=float(cfg.get("recovery_action_timeout",30)))
            except Exception as exc: return {"state":"failed","reason":str(exc)}
            return {"state":"completed" if code == 0 else "failed","reason":"same manager resumed without request replay" if code == 0 else err[-1000:],"stdout":out[-1000:]}
        if name == "refresh_capabilities":
            thread=str(cfg.get("manager_thread_id") or "")
            if not allowed:
                return {"state":"waiting_rollout","reason":"capability candidate refresh requires isolated_active or explicitly enabled guarded_live mode"}
            command="refresh-tools-activate" if mode == "guarded_live" and cfg.get("recovery_live_enabled") is True else "refresh-tools"
            argv=["/usr/bin/python3",str(runtime_root() / "appserver_manager.py"),command,thread]
            env=os.environ | {"CODEX_APP_SERVER_SOCKET":str(cfg.get("app_server_socket") or ""),"CODEX_CONTROL_CONFIG_PATH":str(self.config)}
            try: code,out,err=await bounded_process(argv,env,timeout=float(cfg.get("recovery_action_timeout",30)))
            except Exception as exc: return {"state":"failed","reason":str(exc)}
            return {"state":"completed" if code == 0 else "failed","reason":"verified candidate capability refresh completed" if code == 0 else err[-1000:],"stdout":out[-1000:]}
        commands=cfg.get("recovery_commands") or {}
        argv=commands.get(name)
        # Guarded live mode is deliberately concrete rather than a guessed
        # service name: the operator must explicitly bind each component unit
        # after rollout gates.  This avoids treating a oneshot remote-control
        # registration unit as app-server supervision.
        if (not argv and name.startswith("restart_") and cfg.get("supervisor_mode") == "guarded_live"
                and cfg.get("recovery_live_enabled") is True):
            component=name.removeprefix("restart_")
            unit=(cfg.get("component_units") or {}).get(component)
            if isinstance(unit,str) and __import__("re").fullmatch(r"[A-Za-z0-9_.@-]{1,120}(?:\.service)?",unit):
                argv=["systemctl","--user","restart",unit]
            elif component == "app_server":
                # Codex can own the app-server as its supported daemon rather
                # than a separate systemd unit. Require an explicit absolute
                # executable binding and use its native lifecycle command.
                codex=str(cfg.get("codex_binary") or "")
                if Path(codex).is_absolute() and Path(codex).is_file() and os.access(codex,os.X_OK):
                    argv=[codex,"app-server","daemon","restart"]
        if not isinstance(argv,list) or not argv or not all(isinstance(item,str) for item in argv):
            return {"state":"waiting_rollout","reason":f"no configured isolated command for {name}"}
        if not allowed:
            return {"state":"waiting_rollout","reason":"component restart requires isolated_active or explicitly enabled guarded_live mode"}
        if name.startswith("restart_"):
            component=name.removeprefix("restart_")
            try: self.db.reserve_restart(component,str(action["action_id"]))
            except RuntimeError as exc: return {"state":"blocked","reason":str(exc),"component":component}
        code,out,err=await bounded_process(argv,os.environ | {"CODEX_CONTROL_RECOVERY_ACTION":action["action_id"]},timeout=float(cfg.get("recovery_action_timeout",30)))
        if code: return {"state":"failed","reason":f"{name} exited {code}","stderr":err[-1000:],"stdout":out[-1000:]}
        return {"state":"completed","command":name,"stdout":out[-1000:],"stderr":err[-1000:]}

    async def recover(self, component: str="all", *, idempotency_key: str | None = None, automatic: bool = False) -> dict[str,Any]:
        if automatic and self.db.paused(): return {"state":"waiting_user","reason":"automatic recovery is paused"}
        check=await self.check()
        by_name={item["component"]:item for item in check["components"]}
        # A diagnostic or no-op is not a component restart. The independent
        # reserve_restart ledger enforces limits before each actual restart.
        # A terminal-binding replacement is a distinct, capability-scoped
        # action.  It cannot be authorized merely by guessing an action id for
        # an unrelated restart/reconnect claim.
        kind="recover_manager_binding" if component in {"manager","herdr","all"} else "reconnect_observer"
        action=self.db.claim(component,kind,"automatic threshold recovery" if automatic else "manual recovery",idempotency_key=idempotency_key,admission_budget=False)
        if not action.get("admitted"): return action
        cfg=self.bindings()
        outcome={"check_before":check}
        try:
            aliases={"manager":{"manager_thread","manager_turn","manager_native"},"observer":{"observer"}}
            relevant=(set(by_name) if component == "all" else aliases.get(component,{component}))
            diagnosed={name for name in relevant if name in by_name}
            if diagnosed and all(by_name[name].get("status") == "healthy" for name in diagnosed):
                outcome.update({"state":"completed","note":"requested components are already healthy","verified_check":check})
                self.db.finish(action,outcome,"completed")
                return action | {"state":"completed","outcome":outcome}
            # Observer reconnect is always first and may run in isolated mode only.
            reconnect=await self._run_action(cfg,action,"observer_reconnect")
            outcome["observer_reconnect"]=reconnect
            after=await self.check(); outcome["check_after"]=after
            after_by={item["component"]:item for item in after["components"]}
            # Same-manager continuation is only for a verified transient terminal
        # turn. It runs reconciliation first and never repeats the original prompt.
            authoritative_turn=(after.get("authoritative") or {}).get("turn") or {}
        # Completed/idle/interrupted/approval turns are never continuations.
        # Only the authoritative app-server `failed` terminal with transient
        # error evidence can enter reconciliation then same-manager resume.
            if component in {"all","manager"} and str(authoritative_turn.get("status")) == "failed" and self._transient(authoritative_turn.get("error")):
                action["failed_turn_id"]=str(authoritative_turn.get("id") or "")
                reconcile=await self._run_action(cfg,action,"reconcile_durable_work")
                continuation=await self._run_action(cfg,action,"manager_continuation") if reconcile.get("state")=="completed" else {"state":"blocked","reason":"durable reconciliation did not complete"}
                outcome["manager_recovery"]={"reconcile":reconcile,"continuation":continuation,"never_replayed_original_request":True}
        # Component restart is evidence-gated and unavailable in live
        # observation-only mode. No systemd unit is guessed: config owns argv.
            for name in ("broker","app_server","herdr"):
                item=after_by.get(name,{})
                if component not in {"all",name} or item.get("status") != "unavailable": continue
                reason=str(item.get("reason", ""))
                if not self._restartable_infrastructure(reason):
                    outcome[f"restart:{name}"]={"state":"degraded","reason":"missing positive infrastructure evidence; authentication/quota/provider/unknown errors forbid local restart"}
                    continue
                outcome[f"restart:{name}"]=await self._run_action(cfg,action,f"restart_{name}")
        # Missing tools are a capability gap, never an app-server restart. A
        # guarded refresh forks only a verified candidate via appserver_manager.
            tools_item=after_by.get("broker_tools",{})
            inventory=(after.get("authoritative") or {}).get("control_broker") or {}
            inventory_confirmed=(after_by.get("app_server",{}).get("status") == "healthy"
                                 and inventory.get("inventory_verified") is True)
            if component in {"all","manager"} and tools_item.get("status") == "unavailable" and inventory_confirmed:
                outcome["capability_refresh"]=await self._run_action(cfg,action,"refresh_capabilities")
            restarted={name for name in ("broker","app_server","herdr")
                       if (outcome.get(f"restart:{name}") or {}).get("state") == "completed"}
            if restarted:
                verified=after
                try: readiness_timeout=min(30.0,max(0.0,float(cfg.get("post_restart_readiness_timeout",5))))
                except (TypeError,ValueError): readiness_timeout=5.0
                deadline=time.monotonic()+readiness_timeout
                readiness_work=self._begin_work(readiness_timeout+1)
                try:
                    while any(item.get("status") != "healthy" for item in verified["components"] if item.get("component") in restarted):
                        remaining=deadline-time.monotonic()
                        if remaining <= 0: break
                        await asyncio.sleep(min(.25,remaining))
                        remaining=deadline-time.monotonic()
                        if remaining <= 0: break
                        try: verified=await asyncio.wait_for(self.check(),remaining)
                        except asyncio.TimeoutError: break
                finally: self._end_work(readiness_work)
            else:
                verified=await self.check()
            native_missing=after_by.get("manager_native",{}).get("status") == "unavailable"
            topology_attempted=False
            if "herdr" in restarted or (component in {"all","manager"} and native_missing):
                herdr_ready=next((item.get("status") == "healthy" for item in verified["components"] if item.get("component") == "herdr"),False)
                if herdr_ready:
                    # Only a responsive Herdr socket may be asked to verify the
                    # exact configured canonical pane/thread topology.
                    outcome["manager_topology_reconciliation"]=await self._run_action(cfg,action,"reconcile_manager_topology")
                    topology_attempted=True
                else:
                    outcome["manager_topology_reconciliation"]={"state":"blocked","reason":"Herdr did not become responsive within the bounded readiness deadline"}
            if topology_attempted:
                # Re-read after the claimed broker reattach.  A successful
                # admission alone is not evidence that the configured pane now
                # carries the exact canonical native thread.
                verified=await self.check()
            outcome["verified_check"]=verified
            states=[]
            for step,value in outcome.items():
                # A reconnect attempted before a required app-server restart
                # is expected to fail. Retain that evidence, but let fresh
                # post-restart observer readiness prove that it was repaired.
                if step == "observer_reconnect" and any(item.get("component") == "observer" and item.get("status") == "healthy" for item in verified["components"]):
                    continue
                if isinstance(value,dict) and "state" in value: states.append(value["state"])
            if any(state in {"failed","uncertain"} for state in states): final="failed"
            elif any(state in {"waiting_rollout","blocked","waiting_user"} for state in states): final="blocked"
            else:
                prior={item["component"] for item in check["components"] if item["status"] == "unavailable"}
                current={item["component"]:item["status"] for item in verified["components"]}
            # Only assert recovery for components actually diagnosed. A missing
            # optional binding remains unavailable and therefore prevents a
            # false verified-success outcome for an all-components request.
                relevant=prior if component == "all" else {component}
                aliases={"manager":{"manager_thread","manager_turn","manager_native"}}
                expected=set().union(*(aliases.get(item,{item}) for item in relevant))
                final="completed" if expected and all(current.get(item)=="healthy" for item in expected) else "failed"
            outcome["state"]=final
            if final != "completed": outcome["verification_reason"]="post-action authoritative health did not verify recovered components"
            self.db.finish(action,outcome,final); return action | {"state":final,"outcome":outcome}
        except asyncio.CancelledError:
            outcome["state"]="uncertain"
            outcome["reason"]="caller cancelled recovery; in-flight execution evidence is incomplete and action was not replayed"
            self.db.finish(action,outcome,"uncertain")
            raise
        except Exception as exc:
            # A bounded upstream helper failure is an uncertain action, not a
            # crash of the independent recovery owner. Preserve the evidence
            # and keep health/manual controls responsive during an outage.
            outcome.update(state="uncertain",reason=f"{type(exc).__name__}: {exc}")
            self.db.finish(action,outcome,"uncertain")
            return action | {"state":"uncertain","outcome":outcome}
    async def automatic_recover(self, check: dict[str,Any]) -> dict[str,Any] | None:
        cfg=self.bindings()
        if cfg.get("supervisor_mode") != "isolated_active" and not (
            cfg.get("supervisor_mode") == "guarded_live" and cfg.get("recovery_live_enabled") is True
        ):
            return None
        if self.db.paused() or not any(item["status"] == "unavailable" for item in check["components"]): return None
        return await self.recover("all",automatic=True)
    async def client(self, reader, writer):
        try:
            sock=writer.get_extra_info("socket")
            if sock and hasattr(socket,"SO_PEERCRED"):
                _,uid,_=struct.unpack("3i",sock.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12))
                if uid != os.getuid(): raise PermissionError("owner-only supervisor socket")
            raw=await asyncio.wait_for(reader.readline(),PROBE_TIMEOUT)
            if len(raw) > 8192: raise ValueError("supervisor request exceeds 8192-byte limit")
            req=json.loads(raw)
            if not isinstance(req,dict): raise ValueError("supervisor request must be an object")
            method=req.get("method")
            configured_timeout=self.bindings().get("public_rpc_timeout",90)
            try: timeout=min(90.0,max(1.0,float(configured_timeout)))
            except (TypeError,ValueError): timeout=90.0
            if method=="status": result=self.status()
            elif method=="check": result=await asyncio.wait_for(self.check(),timeout)
            elif method=="recover":
                component=req.get("component","all"); idempotency_key=req.get("idempotency_key")
                if component not in {"all","broker","app_server","herdr","manager","observer"}: raise ValueError("invalid recovery component")
                if idempotency_key is not None and (not isinstance(idempotency_key,str) or len(idempotency_key)>256): raise ValueError("invalid recovery idempotency key")
                result=await asyncio.wait_for(self.recover(component,idempotency_key=idempotency_key),timeout)
            elif method=="pause": self.db.set_paused(True); result=self.status()
            elif method=="resume": self.db.set_paused(False); result=await asyncio.wait_for(self.check(),timeout)
            else: raise ValueError("unknown supervisor method")
            if method == "recover" and isinstance(result,dict):
                result=dict(result); result.pop("claim_key",None)
            reply={"ok":True,"result":result}
        except Exception as exc: reply={"ok":False,"error":str(exc)}
        with contextlib.suppress(ConnectionError, BrokenPipeError):
            writer.write(json.dumps(reply,separators=(",",":")).encode()+b"\n")
            await writer.drain()
        writer.close()
        with contextlib.suppress(ConnectionError, BrokenPipeError): await writer.wait_closed()
    async def serve(self):
        self.socket_path.parent.mkdir(parents=True,exist_ok=True)
        if self.socket_path.exists():
            if not self.socket_path.is_socket() or self.socket_path.stat().st_uid!=os.getuid(): raise RuntimeError("unsafe supervisor socket")
            self.socket_path.unlink()
        old=os.umask(0o077)
        try: server=await asyncio.start_unix_server(self.client,path=str(self.socket_path))
        finally: os.umask(old)
        os.chmod(self.socket_path,0o600)
        systemd_notify("READY=1")
        heartbeat=asyncio.create_task(self._watchdog_heartbeat())
        try:
            async with server:
                while not self.stop.is_set():
                    check=await self.check(); await self.automatic_recover(check)
                    try: await asyncio.wait_for(self.stop.wait(),INTERVAL)
                    except asyncio.TimeoutError: pass
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat,return_exceptions=True)
            if self.observer_process and self.observer_process.returncode is None:
                with contextlib.suppress(ProcessLookupError): os.killpg(self.observer_process.pid,signal.SIGTERM)
                with contextlib.suppress(Exception): await asyncio.wait_for(self.observer_process.wait(),2)
            for task in self.observer_drains: task.cancel()
            await asyncio.gather(*self.observer_drains,return_exceptions=True)

def main() -> int:
    p=argparse.ArgumentParser(); p.add_argument("--socket",type=Path,required=True); p.add_argument("--database",type=Path,required=True); p.add_argument("--config",type=Path,required=True); args=p.parse_args(); asyncio.run(Supervisor(args.socket,args.database,args.config).serve()); return 0
if __name__=='__main__': raise SystemExit(main())
