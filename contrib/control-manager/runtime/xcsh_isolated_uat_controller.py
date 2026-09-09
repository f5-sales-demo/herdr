#!/usr/bin/env python3
"""Durable fail-closed controller for owned disposable XCSH UAT runtimes."""
from __future__ import annotations
import argparse, hashlib, json, os, re, secrets, sqlite3, subprocess, time, uuid
from pathlib import Path
from typing import Any, Callable

ALLOWED = {"reconnect_replay", "generation_supersession", "cleanup", "restart_loss"}
# The released XCSH JSON-mode SessionHeader.id is the producer's canonical
# identity.  It is a 16-character lowercase hexadecimal sessionManager id,
# not a CLI prefix, a path, or a UUID invented by this controller.
CANONICAL_XCSH_SESSION_ID = re.compile(r"^[0-9a-f]{16}$")

class ControllerError(RuntimeError): pass

class IsolatedController:
    def __init__(self, db: Path, ownership: dict[str, Any], token: str, *, clock=time.time):
        self.db, self.ownership, self.token, self.clock = db, ownership, token, clock
        session, service = ownership.get("session_id"), ownership.get("service_id")
        panes = ownership.get("pane_ids")
        if not isinstance(session, str) or not session.startswith("xcsh-uat-") or session in {"default", "control"}:
            raise ControllerError("controller refuses non-disposable/default session")
        if not isinstance(service, str) or not service.startswith("xcsh-uat-"):
            raise ControllerError("controller refuses non-disposable service")
        if not isinstance(panes, list) or not panes or any(not isinstance(p, str) or not p for p in panes):
            raise ControllerError("controller requires declared owned panes")
        self.conn = sqlite3.connect(db, isolation_level=None); self.conn.row_factory = sqlite3.Row
        self.conn.execute("CREATE TABLE IF NOT EXISTS uat_actions(key TEXT PRIMARY KEY, request_sha256 TEXT NOT NULL, state TEXT NOT NULL, receipt TEXT, created REAL NOT NULL)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS uat_executions(execution_id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, tab_id TEXT NOT NULL, pane_id TEXT NOT NULL, receipt TEXT NOT NULL)")

    def register_execution(self, admission: dict[str, Any]) -> dict[str, Any]:
        """Durably bind only the broker's exact admission receipt to this runtime."""
        execution_id,workspace,tab,pane=(admission.get(k) for k in ("id","workspace_id","tab_id","pane_id"))
        if not all(isinstance(x,str) and x for x in (execution_id,workspace,tab,pane)) or workspace != self.ownership.get("workspace_id"):
            raise ControllerError("admission receipt is not for this controller workspace")
        receipt={"execution_id":execution_id,"workspace_id":workspace,"tab_id":tab,"pane_id":pane}
        prior=self.conn.execute("SELECT receipt FROM uat_executions WHERE execution_id=?",(execution_id,)).fetchone()
        if prior:
            if json.loads(prior["receipt"]) != receipt: raise ControllerError("conflicting execution admission receipt")
            return receipt
        self.conn.execute("INSERT INTO uat_executions VALUES(?,?,?,?,?)",(execution_id,workspace,tab,pane,json.dumps(receipt,sort_keys=True)))
        return receipt

    def _registered(self, pane_id: str) -> dict[str, Any]:
        row=self.conn.execute("SELECT * FROM uat_executions WHERE pane_id=?",(pane_id,)).fetchone()
        if not row: raise ControllerError("controller refuses unregistered execution pane")
        return dict(row)

    def act(self, kind: str, pane_id: str, key: str, token: str, hook: Callable[[str, dict[str, Any]], dict[str, Any]]) -> dict[str, Any]:
        if kind not in ALLOWED or token != self.token:
            raise ControllerError("controller action ownership/authentication rejected")
        execution=self._registered(pane_id)
        target={"session_id":self.ownership["session_id"],"service_id":self.ownership["service_id"],"workspace_id":execution["workspace_id"],"execution_id":execution["execution_id"],"tab_id":execution["tab_id"],"pane_id":pane_id}
        request_sha=hashlib.sha256(json.dumps({"kind":kind,"target":target},sort_keys=True).encode()).hexdigest()
        self.conn.execute("BEGIN IMMEDIATE")
        prior=self.conn.execute("SELECT * FROM uat_actions WHERE key=?",(key,)).fetchone()
        if prior:
            self.conn.execute("COMMIT")
            if prior["request_sha256"] != request_sha: raise ControllerError("action key conflicts with a different immutable request")
            if prior["state"] != "completed": raise ControllerError("prior action outcome is uncertain; reconcile before any retry")
            return json.loads(prior["receipt"])
        self.conn.execute("INSERT INTO uat_actions(key,request_sha256,state,receipt,created) VALUES(?,?, 'claimed',NULL,?)",(key,request_sha,self.clock()))
        self.conn.execute("COMMIT")
        receipt=hook(kind,target)
        if not isinstance(receipt,dict) or receipt.get("kind")!=kind or receipt.get("pane_id")!=pane_id or receipt.get("session_id")!=self.ownership["session_id"]:
            raise ControllerError("isolated hook returned invalid action receipt")
        immutable={key:target[key] for key in ("execution_id","workspace_id","tab_id","pane_id","session_id","service_id")}
        if any(receipt.get(key) is not None and receipt.get(key) != value for key,value in immutable.items()):
            raise ControllerError("action receipt conflicts with immutable admitted execution provenance")
        receipt={**receipt,**immutable,"action_key_sha256":hashlib.sha256(key.encode()).hexdigest(),"recorded_at":self.clock()}
        self.conn.execute("BEGIN IMMEDIATE"); self.conn.execute("UPDATE uat_actions SET state='completed',receipt=? WHERE key=?",(json.dumps(receipt,sort_keys=True),key)); self.conn.execute("COMMIT")
        return receipt
    def close(self): self.conn.close()


class DisposableHerdrController(IsolatedController):
    """Real CLI-backed controls, confined to a runtime this object created.

    Caller labels are not ownership: the session, service, workspace and pane
    are taken only from the exact CLI receipts created by :meth:`launch`.
    """
    def __init__(self, db: Path, ownership: dict[str, Any], token: str, binary: Path, *, clock=time.time):
        if not binary.is_file(): raise ControllerError("published Herdr executable is absent")
        super().__init__(db, ownership, token, clock=clock)
        self.binary=binary; self.binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest()

    @staticmethod
    def _call(binary: Path, session: str, *args: str) -> dict[str, Any]:
        call=subprocess.run([str(binary),"--session",session,*args],check=False,capture_output=True,text=True)
        if call.returncode: raise ControllerError(f"owned Herdr command failed: {' '.join(args[:2])}: {call.stderr.strip()}")
        try: value=json.loads(call.stdout)
        except json.JSONDecodeError as exc: raise ControllerError("owned Herdr command did not return JSON") from exc
        if args[:2]==("status","server"):
            if not isinstance(value,dict): raise ControllerError("owned status did not return an object")
            return value
        if not isinstance(value,dict) or not isinstance(value.get("result"),dict):
            raise ControllerError(f"owned Herdr command returned no result: {' '.join(args[:2])}")
        return value["result"]

    @classmethod
    def launch(cls, db: Path, binary: Path, cwd: Path, *, expected_sha256: str, expected_version: str|None=None, timeout: float=12) -> "DisposableHerdrController":
        actual=hashlib.sha256(binary.read_bytes()).hexdigest() if binary.is_file() else ""
        if actual != expected_sha256: raise ControllerError("refusing unmeasured Herdr executable")
        session=service=f"xcsh-uat-{uuid.uuid4().hex[:12]}"
        subprocess.run(["systemd-run","--user","--unit",service,"--collect",str(binary),"--session",session,"server"],check=True,capture_output=True,text=True)
        deadline=time.monotonic()+timeout; status=None
        while time.monotonic()<deadline:
            try:
                status=cls._call(binary,session,"status","server","--json")
                if status.get("running") and status.get("session")==session and (expected_version is None or status.get("version")==expected_version): break
            except ControllerError: pass
            time.sleep(.15)
        else: raise ControllerError("owned disposable Herdr server did not start")
        created=cls._call(binary,session,"workspace","create","--cwd",str(cwd),"--label","xcsh-uat-owned","--no-focus")
        root,workspace=created.get("root_pane"),created.get("workspace")
        if not isinstance(root,dict) or not isinstance(workspace,dict): raise ControllerError("owned workspace creation receipt is incomplete")
        pane,workspace_id=root.get("pane_id"),workspace.get("workspace_id")
        if not isinstance(pane,str) or not isinstance(workspace_id,str): raise ControllerError("owned workspace creation returned no pane identity")
        ownership={"session_id":session,"service_id":service,"workspace_id":workspace_id,"pane_ids":[pane],"created_pane_ids":[pane],"server_socket":status.get("socket"),"binary_sha256":actual,"herdr_version":status.get("version")}
        return cls(db,ownership,secrets.token_urlsafe(32),binary)

    def save_receipt(self, path: Path) -> dict[str,Any]:
        """Persist the local-only controller capability; never put its token in a manifest."""
        payload={"schema_version":1,"ownership":self.ownership,"token":self.token,"binary":str(self.binary),"binary_sha256":self.binary_sha256}
        path.parent.mkdir(parents=True,exist_ok=True)
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,"w") as out: json.dump(payload,out,sort_keys=True)
        return {"controller_receipt":str(path),"workspace_id":self.ownership["workspace_id"],"herdr_socket":self.ownership["server_socket"],"session_id":self.ownership["session_id"]}

    @classmethod
    def open_receipt(cls, db: Path, path: Path) -> "DisposableHerdrController":
        try: payload=json.loads(path.read_text())
        except (OSError,json.JSONDecodeError) as exc: raise ControllerError("controller receipt is unreadable") from exc
        if path.stat().st_mode & 0o077: raise ControllerError("controller receipt must be machine-local mode 0600")
        if payload.get("schema_version")!=1 or not isinstance(payload.get("ownership"),dict) or not isinstance(payload.get("token"),str):
            raise ControllerError("controller receipt schema is invalid")
        controller=cls(db,payload["ownership"],payload["token"],Path(payload.get("binary","")))
        if controller.binary_sha256 != payload.get("binary_sha256"): raise ControllerError("controller receipt executable hash changed")
        return controller

    def _owned_call(self,*args: str) -> dict[str,Any]: return self._call(self.binary,self.ownership["session_id"],*args)

    def register_execution(self, admission: dict[str, Any]) -> dict[str, Any]:
        receipt=super().register_execution(admission)
        pane=self._owned_call("pane","get",receipt["pane_id"]).get("pane",{})
        tab=self._owned_call("tab","get",receipt["tab_id"]).get("tab",{})
        if pane.get("tab_id") != receipt["tab_id"] or pane.get("workspace_id") != receipt["workspace_id"] or tab.get("workspace_id") != receipt["workspace_id"]:
            raise ControllerError("admitted execution provenance does not match live Herdr pane/tab")
        return receipt

    def verify_live_ownership(self) -> None:
        """Reject a caller-crafted label unless the created unit/runtime still match."""
        service=self.ownership["service_id"]
        unit=subprocess.run(["systemctl","--user","show",service,"--property=ExecStart","--property=ActiveState"],check=False,capture_output=True,text=True)
        if unit.returncode or "ActiveState=active" not in unit.stdout or str(self.binary) not in unit.stdout or self.ownership["session_id"] not in unit.stdout:
            raise ControllerError("owned disposable service provenance no longer matches")
        listed=self._owned_call("workspace","list").get("workspaces",[])
        if self.ownership["workspace_id"] not in {item.get("workspace_id") for item in listed if isinstance(item,dict)}:
            raise ControllerError("owned workspace provenance no longer matches")

    def probe_xcsh_json_session(self, xcsh_binary: Path, expected_sha256: str, cwd: Path,
                                session_dir: Path) -> dict[str, Any]:
        """Observe the released XCSH JSON-mode session behavior without a prompt.

        XCSH has no ``--create-session-json`` or capabilities command.  Its
        supported non-interactive ``--mode json --session-dir`` invocation
        emits a SessionHeader first on stdout, before any prompt is submitted.
        The controller uses that observable behavior; it does not accept a
        caller-supplied session id or manifest-declared capability.  The
        returned ``resume_ready`` fact is evidence, not a guessed feature:
        current releases emit a header without durably creating a resume file.
        """
        if (not xcsh_binary.is_file() or hashlib.sha256(xcsh_binary.read_bytes()).hexdigest() != expected_sha256
                or not cwd.is_dir() or not session_dir.is_absolute() or session_dir.exists()):
            raise ControllerError("controller requires a measured XCSH binary and fresh absolute session directory")
        session_dir.mkdir(parents=True, mode=0o700)
        argv = [str(xcsh_binary), "--mode", "json", "--session-dir", str(session_dir),
                "--no-tools", "--no-mcp", "--no-lsp", "--no-memories", "--no-skills", "--no-rules"]
        try:
            call = subprocess.run(argv, cwd=cwd, check=False, capture_output=True, text=True, timeout=20)
        except subprocess.TimeoutExpired as exc:
            raise ControllerError("XCSH JSON-mode session header timed out") from exc
        if call.returncode:
            raise ControllerError(f"owned XCSH JSON-mode session creation failed: {call.stderr.strip()[:500]}")
        header: dict[str, Any] | None = None
        for line in call.stdout.splitlines():
            try:
                candidate = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(candidate, dict) and candidate.get("type") == "session":
                header = candidate
                break
        session = header.get("id") if header else None
        if not isinstance(session, str) or not CANONICAL_XCSH_SESSION_ID.fullmatch(session):
            raise ControllerError("XCSH JSON-mode header lacks canonical sessionManager id")
        if header.get("cwd") != str(cwd.resolve()):
            raise ControllerError("XCSH JSON-mode header cwd does not match the owned invocation")
        files = list(session_dir.rglob("*.jsonl"))
        session_file: str | None = None
        if len(files) == 1:
            try:
                persisted = json.loads(files[0].read_text(encoding="utf-8").splitlines()[0])
            except (OSError, IndexError, json.JSONDecodeError) as exc:
                raise ControllerError("XCSH persisted session header is unreadable") from exc
            identity_fields = ("type", "version", "id", "timestamp", "cwd")
            if any(persisted.get(key) != header.get(key) for key in identity_fields):
                raise ControllerError("XCSH stdout and persisted session headers disagree")
            session_file = str(files[0])
        return {"session_id": session, "session_file": session_file,
                "header_sha256": hashlib.sha256(json.dumps(header, sort_keys=True).encode()).hexdigest(),
                "json_mode_session_header": True, "resume_ready": session_file is not None}

    def create_xcsh_session(self, xcsh_binary: Path, expected_sha256: str, cwd: Path,
                            session_dir: Path) -> dict[str, str]:
        """Return a real, persisted producer session or fail closed."""
        receipt = self.probe_xcsh_json_session(xcsh_binary, expected_sha256, cwd, session_dir)
        if not receipt["resume_ready"]:
            raise ControllerError(
                "released XCSH emitted a JSON session header but did not persist a resume-ready session; "
                "the producer needs a prompt-free durable session creation API"
            )
        return {"session_id": receipt["session_id"], "session_file": receipt["session_file"],
                "header_sha256": receipt["header_sha256"]}

    def _restart(self) -> dict[str,Any]:
        session,service=self.ownership["session_id"],self.ownership["service_id"]
        before=self._owned_call("status","server","--json")
        stopped=subprocess.run([str(self.binary),"--session",session,"server","stop"],check=False,capture_output=True,text=True)
        if stopped.returncode: raise ControllerError(f"owned Herdr stop failed: {stopped.stderr.strip()}")
        subprocess.run(["systemd-run","--user","--unit",service,"--collect",str(self.binary),"--session",session,"server"],check=True,capture_output=True,text=True)
        deadline=time.monotonic()+12
        while time.monotonic()<deadline:
            try:
                after=self._owned_call("status","server","--json")
                if after.get("running") and after.get("session")==session:
                    return {"before_socket":before.get("socket"),"after_socket":after.get("socket"),"server_version":after.get("version"),"stop_exit":stopped.returncode}
            except ControllerError: pass
            time.sleep(.15)
        raise ControllerError("owned Herdr did not reconnect after restart")

    def real_action(self,kind: str,pane_id: str,key: str,token: str,external: Callable[[],dict[str,Any]]|None=None) -> dict[str,Any]:
        """Perform a concrete owned action; external is only a real broker continuation."""
        self.verify_live_ownership()
        def hook(_: str,target: dict[str,Any]) -> dict[str,Any]:
            if kind=="reconnect_replay":
                status=self._owned_call("status","server","--json"); workspaces=self._owned_call("workspace","list")
                effect={"reconnected":bool(status.get("running")),"socket":status.get("socket"),"workspace_count":len(workspaces.get("workspaces",[])),"execution_id":target["execution_id"]}
            elif kind=="restart_loss": effect=self._restart()
            elif kind=="cleanup":
                closed=self._owned_call("tab","close",target["tab_id"])
                effect={"closed_execution_id":target["execution_id"],"closed_tab_id":target["tab_id"],"close_type":closed.get("type")}
            else:
                if external is None: raise ControllerError("generation supersession requires the real broker continuation")
                broker_receipt=external()
                if not isinstance(broker_receipt,dict) or not broker_receipt.get("id"): raise ControllerError("broker continuation returned no task receipt")
                effect={"broker_task_id":broker_receipt["id"],"broker_state":broker_receipt.get("state")}
            return {"kind":kind,"session_id":target["session_id"],"pane_id":target["pane_id"],"effect":effect,"binary_sha256":self.binary_sha256}
        return self.act(kind,pane_id,key,token,hook)


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare",action="store_true",help="create an owned disposable Herdr controller receipt")
    parser.add_argument("--binary",type=Path,required=True)
    parser.add_argument("--sha256",required=True)
    parser.add_argument("--version",required=True)
    parser.add_argument("--cwd",type=Path,required=True)
    parser.add_argument("--state-db",type=Path,required=True)
    parser.add_argument("--receipt",type=Path,required=True)
    args=parser.parse_args()
    if not args.prepare: parser.error("--prepare is required")
    controller=DisposableHerdrController.launch(args.state_db,args.binary,args.cwd,expected_sha256=args.sha256,expected_version=args.version)
    try: print(json.dumps(controller.save_receipt(args.receipt),sort_keys=True))
    finally: controller.close()
    return 0


if __name__ == "__main__": raise SystemExit(main())
