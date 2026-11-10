"""Increment 04 failure-recovery gate. Python 3 stdlib + Docker Compose."""
import concurrent.futures
import json
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
BASE = {"node-a": "http://127.0.0.1:5001", "node-b": "http://127.0.0.1:5002", "node-c": "http://127.0.0.1:5003"}
OUT = ROOT / ".artifacts" / "step04"
OUT.mkdir(parents=True, exist_ok=True)


def compose(*args):
    result = subprocess.run(["docker", "compose", *args], cwd=ROOT, text=True,
                            encoding="utf-8", capture_output=True, timeout=300)
    if result.returncode:
        raise RuntimeError(result.stderr + result.stdout)
    return result.stdout


def call(node, route, method="GET", key=None, duration=None):
    data = None if duration is None else json.dumps({"durationSeconds": duration}).encode()
    req = urllib.request.Request(BASE[node] + route, data=data, method=method,
                                 headers={"Idempotency-Key": key} if key else {})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=8) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        body = error.read()
        return error.code, json.loads(body) if body and error.headers.get_content_type() == "application/json" else None


def wait(label, predicate, seconds=60, interval=.5):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                print("PASS", label, flush=True)
                return value
        except (AssertionError, OSError, urllib.error.URLError, KeyError, TypeError) as error:
            last = repr(error)
        time.sleep(interval)
    raise AssertionError(f"{label}: timed out; last={last}")


def cluster(nodes):
    rows = {node: call(node, "/cluster")[1] for node in nodes}
    leaders = [row for row in rows.values() if row["role"] == "leader"]
    if len(leaders) != 1 or not all(row["ready"] and row["leaderId"] == leaders[0]["nodeId"] for row in rows.values()):
        return None
    return leaders[0]


def state(payment_id, nodes):
    for node in nodes:
        status, body = call(node, "/payments/" + payment_id)
        assert status == 200, (node, status, body)
        assert body["transactionId"] == payment_id
    return body


def submit(node, key, duration=10):
    status, body = call(node, "/pay", "POST", key, duration)
    assert status == 202, (node, status, body)
    return body["transactionId"]


def completed(payment_id, nodes, attempt=None):
    body = state(payment_id, nodes)
    assert body["status"] == "Completed", body
    assert body["result"] == "Succeeded" and body["completedAt"] is not None, body
    if attempt is not None:
        assert body["attempt"] == attempt, body
    return body


def logs():
    return compose("logs", "--no-color")


def event_lines(payment_id, event_type, attempt=None, node=None):
    return [line for line in logs().splitlines()
            if f"TransactionId={payment_id}" in line and f"EventType={event_type}" in line
            and (attempt is None or f"Attempt={attempt}" in line)
            and (node is None or f"NodeId={node}" in line)]


def worker_started(payment_id, attempt=1, node=None):
    return bool(event_lines(payment_id, "WorkerStarted", attempt, node))


def completion_event_count(payment_id):
    return len(event_lines(payment_id, "PaymentCompleted"))


def assert_single_completion(payment_id):
    def observed_count():
        count = completion_event_count(payment_id)
        return count if count >= 1 else None
    count = wait("one Raft completion event", observed_count, 10)
    assert count == 1, f"{payment_id} has {count} confirmed completion log events"
    return count


def await_reassignment(payment_id, surviving_nodes, old_node):
    def changed():
        body = state(payment_id, surviving_nodes)
        assert body["status"] == "Processing", body
        if body["attempt"] >= 2:
            assert body["attempt"] == 2 and body["assignedNodeId"] != old_node, body
            return body
        return None
    return wait("Raft confirms Attempt=2 on another Worker", changed, 45)


def restart_all():
    compose("start")
    return wait("three-node cluster healthy after restart", lambda: cluster(list(BASE)), 90)


def main():
    run_id = str(time.time_ns())
    evidence = {"passed": False, "runId": run_id,
                "startedUtc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "scenarios": {}}
    dead = set()
    paused = set()
    try:
        compose("up", "--build", "-d")
        bad = OUT / "incompatible"
        bad.mkdir(exist_ok=True)
        marker = bad / "format.txt"
        marker.write_bytes(b"solidarity-grid-payments-v2\n")
        before = marker.read_bytes()
        probe = subprocess.run(["docker", "run", "--rm", "--network", "none", "-v",
                                f"{bad.resolve()}:/data", "-e", "Grid__DataPath=/data",
                                "solidarity-grid:step04"], text=True, encoding="utf-8",
                               capture_output=True, timeout=30)
        assert probe.returncode != 0 and "Incompatible storage format" in probe.stderr and marker.read_bytes() == before
        evidence["incompatibleFormat"] = "v2 rejected unchanged by v3 binary"
        print("PASS v2 format rejected without changing marker", flush=True)
        initial = wait("three-node Raft election", lambda: cluster(list(BASE)), 90)
        evidence["initialCluster"] = initial
        evidence["initialPings"] = 6
        for sender in BASE:
            for target in BASE:
                if sender != target:
                    code, response = call(sender, "/diagnostics/ping/" + target, "POST")
                    assert code == 200 and response["receiverId"] == target, (sender, target, code, response)
        print("PASS six IMessageBus exchanges", flush=True)

        # Scenario A: kill the Worker three seconds after the confirmed start event.
        failed_worker_tx = submit(initial["nodeId"], f"step04-worker-loss-{run_id}", 10)
        processing = wait("worker-loss transaction assigned", lambda: (lambda p: p if p["status"] == "Processing" else None)(state(failed_worker_tx, list(BASE))))
        worker = processing["assignedNodeId"]
        wait("Worker confirms processing started", lambda: worker_started(failed_worker_tx, 1, worker), 20)
        time.sleep(3)  # Intentional chaos point: three seconds after observable WorkerStarted.
        failure_time = time.monotonic()
        compose("kill", worker)
        dead.add(worker)
        survivors = [node for node in BASE if node != worker]
        reassigned = await_reassignment(failed_worker_tx, survivors, worker)
        recovery = wait("reassigned transaction completed", lambda: (lambda p: p if p["status"] == "Completed" else None)(state(failed_worker_tx, survivors)), 40)
        assert recovery["attempt"] == 2, recovery
        recovery["completionEvents"] = assert_single_completion(failed_worker_tx)
        elapsed = round(time.monotonic() - failure_time, 2)
        evidence["scenarios"]["workerFailure"] = {
            "transactionId": failed_worker_tx, "failedWorker": worker,
            "newWorker": reassigned["assignedNodeId"], "attempt": reassigned["attempt"],
            "completedAttempt": recovery["attempt"], "recoverySeconds": elapsed
        }
        print("PASS worker failure recovered automatically", flush=True)

        # Restore the failed worker with the same v3 volume before leader-loss scenario.
        compose("start", worker)
        dead.remove(worker)
        wait("cluster recovered after Worker restart", lambda: cluster(list(BASE)), 90)

        # Scenario B: kill the current leader while processing. The surviving Worker
        # continues the same generation; if it was the leader's own Worker, fencing
        # and recovery produce the next generation.
        leader = wait("leader before leader-loss scenario", lambda: cluster(list(BASE)), 60)
        leader_tx = submit(next(node for node in BASE if node != leader["nodeId"]), f"step04-leader-loss-{run_id}", 10)
        leader_work = wait("leader-loss transaction assigned", lambda: (lambda p: p if p["status"] == "Processing" else None)(state(leader_tx, list(BASE))))
        wait("leader-loss Worker started", lambda: worker_started(leader_tx), 20)
        time.sleep(2)
        failure_time = time.monotonic()
        compose("kill", leader["nodeId"])
        dead.add(leader["nodeId"])
        survivors = [node for node in BASE if node != leader["nodeId"]]
        replacement = wait("new leader elected after failure", lambda: cluster(survivors), 60)
        assert replacement["nodeId"] != leader["nodeId"] and replacement["term"] > leader["term"], replacement
        leader_result = wait("pending transaction completed after leader loss",
                             lambda: (lambda p: p if p["status"] == "Completed" else None)(state(leader_tx, survivors)), 50)
        assert leader_result["attempt"] in (1, 2)
        leader_result["completionEvents"] = assert_single_completion(leader_tx)
        evidence["scenarios"]["leaderFailure"] = {
            "oldLeader": leader["nodeId"], "newLeader": replacement["nodeId"], "oldWorker": leader_work["assignedNodeId"],
            "attempt": leader_result["attempt"], "completedAt": leader_result["completedAt"],
            "recoverySeconds": round(time.monotonic() - failure_time, 2)
        }
        print("PASS leader failure recovered without operator action", flush=True)

        compose("start", leader["nodeId"])
        dead.remove(leader["nodeId"])
        wait("all nodes rejoined after leader restart", lambda: cluster(list(BASE)), 90)

        # Scenario C: pause a follower Worker after it starts, let the Supervisor
        # confirm Attempt=2, then resume the stale process before Attempt=2 finishes.
        active_leader = wait("leader before delayed Worker scenario", lambda: cluster(list(BASE)), 60)
        delayed_tx = None
        delayed_payment = None
        for attempt in range(4):
            candidate_id = submit(active_leader["nodeId"], f"step04-delayed-{run_id}-{attempt}", 10)
            candidate = wait("delayed Worker assignment", lambda pid=candidate_id: (lambda p: p if p["status"] == "Processing" else None)(state(pid, list(BASE))), 20)
            if candidate["assignedNodeId"] != active_leader["nodeId"]:
                delayed_tx, delayed_payment = candidate_id, candidate
                break
            wait("leader Worker candidate completes", lambda pid=candidate_id: (lambda p: p if p["status"] == "Completed" else None)(state(pid, list(BASE))), 25)
        assert delayed_tx is not None, "Could not obtain a follower Worker for delayed-response scenario."
        delayed_worker = delayed_payment["assignedNodeId"]
        wait("delayed Worker started", lambda: worker_started(delayed_tx), 20)
        time.sleep(2)
        failure_time = time.monotonic()
        compose("pause", delayed_worker)
        paused.add(delayed_worker)
        reassigned = await_reassignment(delayed_tx, [node for node in BASE if node != delayed_worker], delayed_worker)
        compose("unpause", delayed_worker)
        paused.remove(delayed_worker)
        wait("old Worker attempt reported Obsolete", lambda: event_lines(delayed_tx, "ObsoleteAttemptRejected", 1, delayed_worker), 20)
        delayed_result = wait("new generation completed after stale result", lambda: (lambda p: p if p["status"] == "Completed" else None)(state(delayed_tx, list(BASE))), 35)
        assert delayed_result["attempt"] == 2 and delayed_result["assignedNodeId"] == reassigned["assignedNodeId"]
        delayed_result["completionEvents"] = assert_single_completion(delayed_tx)
        evidence["scenarios"]["staleWorker"] = {
            "transactionId": delayed_tx, "oldWorker": delayed_worker,
            "newWorker": delayed_result["assignedNodeId"], "attempt": delayed_result["attempt"],
            "completedAt": delayed_result["completedAt"], "recoverySeconds": round(time.monotonic() - failure_time, 2),
            "oldAttemptRejected": True
        }
        print("PASS resumed obsolete Worker result rejected", flush=True)

        # Scenario D: concurrent distinct work; assignment and execution must remain live.
        concurrent_start = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=9) as pool:
            ids = list(pool.map(lambda i: submit(list(BASE)[i % 3], f"step04-concurrent-{run_id}-{i}", 5 + i % 3), range(9)))
        results = [wait("concurrent transaction completed", lambda pid=pid: (lambda p: p if p["status"] == "Completed" else None)(state(pid, list(BASE))), 55)
                   for pid in ids]
        completion_counts = {pid: assert_single_completion(pid) for pid in ids}
        assert all(item["attempt"] >= 1 for item in results)
        workers = sorted({item["assignedNodeId"] for item in results})
        assert workers == sorted(BASE), workers
        start_counts = {pid: 0 for pid in ids}
        active_max = {node: 0 for node in BASE}
        for line in logs().splitlines():
            if "Payment Worker started" not in line or "EventType=WorkerStarted" not in line:
                continue
            txid = next((pid for pid in ids if f"TransactionId={pid}" in line), None)
            if txid is None:
                continue
            start_counts[txid] += 1
            node_match = re.search(r"NodeId=(node-[abc])", line)
            active_match = re.search(r"ActiveCount=(\d+)", line)
            assert node_match and active_match, line
            active_max[node_match.group(1)] = max(active_max[node_match.group(1)], int(active_match.group(1)))
        assert all(count == 1 for count in start_counts.values()), start_counts
        assert all(count <= 2 for count in active_max.values()), active_max
        evidence["scenarios"]["concurrentWork"] = {
            "transactions": ids, "workers": workers,
            "elapsedSeconds": round(time.monotonic() - concurrent_start, 2),
            "activeCapacityPerNode": 2, "observedActiveMaximumPerNode": active_max,
            "workerStartsPerTransaction": start_counts, "completionEventsPerTransaction": completion_counts
        }
        print("PASS concurrent work completed across all three Workers", flush=True)

        # Scenario E: kill both the current leader and the assigned Worker, leaving
        # one replica. It must neither reassign nor confirm completion without quorum.
        leader = wait("leader before quorum-loss scenario", lambda: cluster(list(BASE)), 60)
        quorum_tx = submit(leader["nodeId"], f"step04-no-quorum-processing-{run_id}", 10)
        quorum_work = wait("no-quorum transaction assigned", lambda: (lambda p: p if p["status"] == "Processing" else None)(state(quorum_tx, list(BASE))))
        wait("no-quorum Worker started", lambda: worker_started(quorum_tx), 20)
        time.sleep(2)
        single_node = next(node for node in BASE if node not in (leader["nodeId"], quorum_work["assignedNodeId"]))
        killed_for_quorum = [node for node in BASE if node != single_node]
        compose("kill", *killed_for_quorum)
        dead.update(killed_for_quorum)
        def no_quorum():
            assert call(single_node, "/health/ready")[0] == 503
            assert call(single_node, "/pay", "POST", f"step04-no-quorum-{run_id}", 5)[0] == 503
            assert call(single_node, "/payments/" + quorum_tx)[0] == 503
            return True
        wait("single node cannot confirm payment without quorum", no_quorum, 25)
        time.sleep(11)  # Let the killed Worker’s original ten-second duration elapse.
        isolation_logs = logs()
        transaction_logs = [line for line in isolation_logs.splitlines() if f"TransactionId={quorum_tx}" in line]
        assert not any("EventType=ReassignmentConfirmed" in line or "EventType=PaymentCompleted" in line
                       for line in transaction_logs), transaction_logs
        evidence["scenarios"]["noQuorum"] = {
            "availableNode": single_node, "failedLeader": leader["nodeId"],
            "failedWorker": quorum_work["assignedNodeId"], "newPaymentStatus": 503,
            "consistentReadStatus": 503, "confirmedReassignment": False, "confirmedCompletion": False
        }

        compose("start", *killed_for_quorum)
        dead.difference_update(killed_for_quorum)
        wait("cluster recovered after quorum restoration", lambda: cluster(list(BASE)), 90)
        quorum_completed = wait("transaction completed after quorum restoration",
                                lambda: (lambda p: p if p["status"] == "Completed" else None)(state(quorum_tx, list(BASE))), 40)
        assert quorum_completed["attempt"] in (1, 2)
        quorum_completed["completionEvents"] = assert_single_completion(quorum_tx)
        evidence["scenarios"]["quorumRecovery"] = {
            "transactionId": quorum_tx, "worker": quorum_completed["assignedNodeId"],
            "attempt": quorum_completed["attempt"], "completedAt": quorum_completed["completedAt"]
        }

        # Scenario F: full restart with the persistent v3 volumes.
        compose("kill", *BASE.keys())
        dead.update(BASE.keys())
        compose("start")
        restart = wait("Raft election after full restart", lambda: cluster(list(BASE)), 90)
        dead.clear()
        restored = {tx: completed(tx, list(BASE)) for tx in [failed_worker_tx, leader_tx, delayed_tx, quorum_tx, *ids]}
        evidence["scenarios"]["restartPersistence"] = {
            "leader": restart["nodeId"], "term": restart["term"],
            "restoredTransactions": len(restored),
            "attempts": {tx: payment["attempt"] for tx, payment in restored.items()}
        }
        evidence["passed"] = True
        print("PASS v3 WAL/snapshot recovery and all chaos scenarios", flush=True)
    finally:
        try:
            if paused:
                compose("unpause", *paused)
            compose("start")
            evidence["finalContainers"] = compose("ps", "--all")
            (OUT / "compose.log").write_text(compose("logs", "--no-color"), encoding="utf-8")
        finally:
            evidence["finishedUtc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            (OUT / "resilience.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
