"""Single-command local recovery demonstration. Keeps all persistent volumes."""
import json
import importlib.util
import time
from pathlib import Path

spec = importlib.util.spec_from_file_location("verify_resilience", Path(__file__).with_name("verify-resilience.py"))
resilience = importlib.util.module_from_spec(spec)
spec.loader.exec_module(resilience)
BASE, cluster, compose = resilience.BASE, resilience.cluster, resilience.compose
event_lines, state, submit, wait = resilience.event_lines, resilience.state, resilience.submit, resilience.wait

OUT = Path(__file__).resolve().parents[1] / ".artifacts" / "step05"
OUT.mkdir(parents=True, exist_ok=True)


def main():
    compose("up", "--build", "-d")
    initial = wait("three-node cluster ready", lambda: cluster(list(BASE)), 90)
    tx = submit(initial["nodeId"], "step05-demo-" + str(time.time_ns()), 10)
    assigned = wait("assignment confirmed", lambda: (lambda p: p if p["status"] == "Processing" else None)(state(tx, list(BASE))), 30)
    owner = assigned["assignedNodeId"]
    wait("worker started", lambda: event_lines(tx, "WorkerStarted", 1, owner), 30)
    time.sleep(3)
    failed_at = time.monotonic()
    compose("kill", owner)
    try:
        survivors = [n for n in BASE if n != owner]
        replacement = wait("Raft reassigned to attempt 2", lambda: (lambda p: p if p["attempt"] == 2 else None)(state(tx, survivors)), 50)
        result = wait("transaction completed", lambda: (lambda p: p if p["status"] == "Completed" else None)(state(tx, survivors)), 50)
        assert replacement["assignedNodeId"] != owner and result["attempt"] == 2
        summary = {"transactionId": tx, "initialLeader": initial["nodeId"], "failedWorker": owner,
                   "newWorker": result["assignedNodeId"], "attempt": result["attempt"],
                   "status": result["status"], "recoverySeconds": round(time.monotonic() - failed_at, 2)}
    finally:
        compose("start", owner)
    summary["finalCluster"] = wait("three-node cluster restored", lambda: cluster(list(BASE)), 90)
    (OUT / "demo.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
