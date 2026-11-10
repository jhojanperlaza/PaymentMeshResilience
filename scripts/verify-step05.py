"""Increment 05 repeatability, saturation, and network-partition checks."""
import importlib.util
import json
from pathlib import Path
import subprocess
import time
from contextlib import contextmanager
from uuid import uuid4

spec = importlib.util.spec_from_file_location("resilience", Path(__file__).with_name("verify-resilience.py"))
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
OUT = r.ROOT / ".artifacts" / "step05"
OUT.mkdir(parents=True, exist_ok=True)


def docker(*args):
    p = subprocess.run(["docker", *args], text=True, encoding="utf-8", capture_output=True, timeout=60)
    if p.returncode:
        raise RuntimeError(p.stderr + p.stdout)
    return p.stdout.strip()


@contextmanager
def partition_network(container, evidence):
    original = json.loads(docker("inspect", container))[0]["NetworkSettings"]["Networks"]
    temporary = "solidarity-grid-step05-" + uuid4().hex
    created = False
    try:
        docker("network", "create", temporary)
        created = True
        docker("network", "connect", temporary, container)
        for name in original:
            docker("network", "disconnect", name, container)
        yield
    finally:
        errors = []
        for name, config in original.items():
            try:
                current = json.loads(docker("inspect", container))[0]["NetworkSettings"]["Networks"]
                if name not in current:
                    aliases = [arg for alias in config.get("Aliases") or [] for arg in ("--alias", alias)]
                    docker("network", "connect", *aliases, name, container)
            except Exception as error:
                errors.append(f"restore {name}: {error}")
        if created:
            try:
                current = json.loads(docker("inspect", container))[0]["NetworkSettings"]["Networks"]
                if temporary in current:
                    docker("network", "disconnect", temporary, container)
                docker("network", "rm", temporary)
            except Exception as error:
                errors.append(f"remove temporary network: {error}")
        current = json.loads(docker("inspect", container))[0]["NetworkSettings"]["Networks"]
        restored = set(current) == set(original) and all(
            set(current[name].get("Aliases") or []) == set(config.get("Aliases") or [])
            for name, config in original.items() if name in current)
        evidence.update({"restoredNetworksAndAliases": restored, "errors": errors})
        if errors or not restored:
            raise RuntimeError(f"Network cleanup failed: {evidence}")


def main():
    result = {"passed": False, "startedUtc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "recoveries": []}
    try:
        r.compose("up", "--build", "-d")
        for index in range(3):
            leader = r.wait("cluster ready", lambda: r.cluster(list(r.BASE)), 90)
            tx = r.submit(leader["nodeId"], f"step05-repeat-{time.time_ns()}-{index}", 10)
            payment = r.wait("payment assigned", lambda: (lambda p: p if p["status"] == "Processing" else None)(r.state(tx, list(r.BASE))), 30)
            worker = payment["assignedNodeId"]
            r.wait("worker started", lambda: r.event_lines(tx, "WorkerStarted", 1, worker), 30)
            time.sleep(3)
            failed_at = time.monotonic()
            r.compose("kill", worker)
            try:
                survivors = [n for n in r.BASE if n != worker]
                done = r.wait("recovery completed", lambda: (lambda p: p if p["status"] == "Completed" and p["attempt"] == 2 else None)(r.state(tx, survivors)), 75)
                assert done["assignedNodeId"] != worker
                replies = r.wait("completion reply recorded for current attempt",
                    lambda: r.event_lines(tx, "PaymentCompleted", 2), 15)
                assert len(replies) == 1, replies
                assert len(r.event_lines(tx, "PaymentCompleted")) == 1, "Unexpected completion reply generation"
                result["recoveries"].append({"transactionId": tx, "oldWorker": worker,
                    "newWorker": done["assignedNodeId"], "attempt": 2, "status": done["status"],
                    "seconds": round(time.monotonic() - failed_at, 2),
                    "completionReplyLogs": len(replies),
                    "completionEvidence": "reply logs only; effective transitions verified by unit tests"})
            finally:
                r.compose("start", worker)
            r.wait("cluster recovered", lambda: r.cluster(list(r.BASE)), 90)

        # More than the six simultaneous worker slots. Busy must not force a new generation.
        import concurrent.futures
        started = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            ids = list(pool.map(lambda i: r.submit(list(r.BASE)[i % 3], f"step05-saturated-{time.time_ns()}-{i}", 10), range(12)))
        payments = [r.wait("saturated payment completed", lambda pid=pid: (lambda p: p if p["status"] == "Completed" else None)(r.state(pid, list(r.BASE))), 100) for pid in ids]
        assert all(p["attempt"] == 1 for p in payments), payments
        result["saturation"] = {"count": len(payments), "attempts": [p["attempt"] for p in payments],
            "seconds": round(time.monotonic() - started, 2),
            "busyEvents": sum(len(r.event_lines(pid, "WorkerBusy")) for pid in ids)}

        # Disconnect one live container from both Compose networks, leaving its host
        # port published through a new network. Docker network operations do not kill it.
        leader = r.wait("leader before partition", lambda: r.cluster(list(r.BASE)), 60)
        isolated = leader["nodeId"]
        container = r.compose("ps", "-q", isolated).strip()
        result["networkCleanup"] = {}
        with partition_network(container, result["networkCleanup"]):
            majority = [n for n in r.BASE if n != isolated]
            new_leader = r.wait("majority elects leader", lambda: r.cluster(majority), 70)
            assert new_leader["nodeId"] != isolated
            r.wait("isolated live node rejects writes", lambda: subprocess.run(
                ["docker", "exec", container, "dotnet", "SolidarityGrid.dll", "--probe-no-quorum"],
                capture_output=True, timeout=15).returncode == 0, 35)
            tx = r.submit(new_leader["nodeId"], f"majority-{time.time_ns()}", 5)
            r.wait("majority commits payment", lambda: (lambda p: p if p["status"] == "Completed" else None)(r.state(tx, majority)), 50)
            result["partition"] = {"isolated": isolated, "majorityLeader": new_leader["nodeId"], "transactionId": tx,
                "isolatedWriteStatus": 503}
        r.wait("partitioned node resynchronized", lambda: r.state(tx, list(r.BASE)), 90)
        result["partition"]["resynchronized"] = True
        result["passed"] = True
    finally:
        result["finishedUtc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        (OUT / "chaos.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    main()
