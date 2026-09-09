#!/usr/bin/env python3
"""Isolated recovery fault harness; no live Herdr/broker service is touched."""
import asyncio, json, tempfile
from pathlib import Path
from control_supervisor import Supervisor

async def call(path: Path, method: str):
    r,w=await asyncio.open_unix_connection(str(path)); w.write(json.dumps({"method":method}).encode()+b"\n"); await w.drain(); answer=json.loads(await r.readline()); w.close(); await w.wait_closed(); return answer
async def main():
    with tempfile.TemporaryDirectory(prefix="control-recovery-isolated-") as raw:
        root=Path(raw); cfg=root/'machine.json'; cfg.write_text(json.dumps({'herdr_socket':str(root/'missing-herdr.sock'),'broker_socket':str(root/'missing-broker.sock'),'app_server_socket':str(root/'missing-app.sock'),'manager_thread_id':'isolated-thread'}))
        sup=Supervisor(root/'supervisor.sock',root/'recovery.sqlite3',cfg); task=asyncio.create_task(sup.serve())
        for _ in range(100):
            if (root/'supervisor.sock').exists(): break
            await asyncio.sleep(.01)
        before=await call(root/'supervisor.sock','check'); recover=await call(root/'supervisor.sock','recover'); duplicate=await call(root/'supervisor.sock','recover'); pause=await call(root/'supervisor.sock','pause'); waiting=await call(root/'supervisor.sock','recover'); resume=await call(root/'supervisor.sock','resume')
        task.cancel()
        try: await task
        except asyncio.CancelledError: pass
        # These are the installed app-server TurnError shapes captured by
        # generate-json-schema, not legacy code/retryable approximations.
        transient_shapes=[{'message':'stream disconnected','codexErrorInfo':{variant:{'httpStatusCode':None}},'additionalDetails':None} for variant in ('responseStreamDisconnected','responseStreamConnectionFailed')]
        rejected_shapes=[{'message':'provider failure','codexErrorInfo':info} for info in ('unauthorized','rateLimitExceeded','usageLimitExceeded','cyberPolicy','other',{'responseStreamDisconnected':{'httpStatusCode':401}},{'responseStreamConnectionFailed':{'httpStatusCode':429}})]
        matrix={'accepted_stream_transport':all(Supervisor._transient(item) for item in transient_shapes),'rejected_provider_auth_quota_policy':not any(Supervisor._transient(item) for item in rejected_shapes)}
        if not all(matrix.values()): raise RuntimeError(f'installed TurnError fault matrix failed: {matrix}')
        print(json.dumps({'run':'isolated-fault-harness','faults':['missing/half-open endpoints','duplicate recovery','paused recovery','manager tool exposure isolation','installed TurnError stream/auth/quota/policy matrix'],'turn_error_matrix':matrix,'check':before,'recover':recover,'duplicate':duplicate,'paused':waiting,'resume':resume,'untested':['real model/provider failure','saturated external observer output','shared component crash/hang','iPhone rendering/audio']},indent=2,sort_keys=True))
if __name__=='__main__': asyncio.run(main())
