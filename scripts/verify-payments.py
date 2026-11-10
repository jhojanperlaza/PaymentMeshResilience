"""Increment 03 integration gate. Python 3 stdlib + Docker Compose."""
import concurrent.futures
import json
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
BASE = {"node-a": "http://127.0.0.1:5001", "node-b": "http://127.0.0.1:5002", "node-c": "http://127.0.0.1:5003"}
OUT = ROOT / ".artifacts" / "step03"
OUT.mkdir(parents=True, exist_ok=True)


def compose(*args):
    p = subprocess.run(["docker", "compose", *args], cwd=ROOT, text=True,
                       encoding="utf-8", capture_output=True, timeout=300)
    if p.returncode:
        raise RuntimeError(p.stderr + p.stdout)
    return p.stdout


def call(node, route, method="GET", key=None, duration=None):
    data = None if duration is None else json.dumps({"durationSeconds": duration}).encode()
    req = urllib.request.Request(BASE[node] + route, data=data, method=method,
                                 headers={"Idempotency-Key": key} if key else {})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=8) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read()
        return e.code, json.loads(body) if body and e.headers.get_content_type() == "application/json" else None


def wait(label, assertion, seconds=60):
    until = time.monotonic() + seconds
    last = None
    while time.monotonic() < until:
        try:
            result = assertion()
            print("PASS", label, flush=True)
            return result
        except (AssertionError, OSError, urllib.error.URLError) as e:
            last = e
            time.sleep(.5)
    raise AssertionError(f"{label}: {last}")


def cluster(nodes):
    rows = [call(n, "/cluster")[1] for n in nodes]
    leaders = [r for r in rows if r["role"] == "leader"]
    assert len(leaders) == 1 and all(r["ready"] and r["leaderId"] == leaders[0]["nodeId"] for r in rows), rows
    return leaders[0]


def payment_everywhere(payment_id, nodes):
    rows = [call(n, "/payments/" + payment_id) for n in nodes]
    assert all(status == 200 and body["transactionId"] == payment_id and
               body["status"] == "Completed" and body["attempt"] == 1 and
               body["assignedNodeId"] in BASE and body["durationSeconds"] in range(5, 11) and
               body["result"] == "Succeeded" for status, body in rows), rows
    return rows


def payment_status(payment_id, expected):
    status, body = call("node-a", "/payments/" + payment_id)
    assert status == 200 and body["status"] == expected and body["attempt"] == 1 and body["assignedNodeId"] in BASE, (status, body)
    return body


def ping_everywhere(nodes):
    for sender in nodes:
        for receiver in nodes:
            if sender != receiver:
                status, pong = call(sender, "/diagnostics/ping/" + receiver, "POST")
                assert status == 200 and pong["receiverId"] == receiver, (status, pong)
    return len(nodes) * (len(nodes) - 1)


def main():
    evidence = {"passed": False, "startedUtc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    try:
        compose("up", "--build", "-d")
        bad = OUT / "incompatible"
        bad.mkdir(exist_ok=True)
        marker = bad / "format.txt"
        marker.write_bytes(b"unknown-format\n")
        before = marker.read_bytes()
        probe = subprocess.run(["docker", "run", "--rm", "--network", "none", "-v",
                                f"{bad.resolve()}:/data", "-e", "Grid__DataPath=/data",
                                "solidarity-grid:step03"], text=True, encoding="utf-8",
                               capture_output=True, timeout=30)
        assert probe.returncode != 0 and "Incompatible storage format" in probe.stderr and marker.read_bytes() == before, probe.stderr
        evidence["incompatibleStartup"] = "rejected without modifying marker"
        print("PASS incompatible volume rejects startup without modification", flush=True)
        old = wait("three-node election", lambda: cluster(BASE))
        evidence["firstLeader"] = old
        evidence["initialPings"] = ping_everywhere(BASE)
        print("PASS six directed IMessageBus ping/pong exchanges", flush=True)
        key = "step02-" + str(time.time_ns())
        codes = []
        for node in BASE:
            status, body = call(node, "/pay", "POST", key, 10)
            assert status == 202, (node, status, body)
            codes.append(body["transactionId"])
        assert len(set(codes)) == 1
        payment_id = codes[0]
        evidence["transactionId"] = payment_id
        processing = wait("accepted payment assigned to a worker", lambda: payment_status(payment_id, "Processing"), 5)
        evidence["firstAssignment"] = {"nodeId": processing["assignedNodeId"], "attempt": processing["attempt"]}
        assert call("node-a", "/pay", "POST", None, 7)[0] == 400
        assert call("node-a", "/pay", "POST", key + "-bad", 4)[0] == 400
        assert call("node-a", "/pay", "POST", key + "-default")[0] == 202
        wait("completed transaction is readable on every node", lambda: payment_everywhere(payment_id, BASE), 30)
        assert call("node-a", "/pay", "POST", key, 8)[0] == 409
        print("PASS conflicting content is 409", flush=True)
        assert call("node-b", "/payments/00000000-0000-0000-0000-000000000001")[0] == 404
        print("PASS unknown transaction is 404", flush=True)
        concurrent_key = key + "-concurrent"
        with concurrent.futures.ThreadPoolExecutor(max_workers=18) as pool:
            results = list(pool.map(lambda i: call(list(BASE)[i % 3], "/pay", "POST", concurrent_key, 6), range(36)))
        assert all(status == 202 for status, _ in results), results
        ids = {body["transactionId"] for _, body in results}
        assert len(ids) == 1
        evidence["concurrentRequests"] = len(results)
        evidence["concurrentTransactionId"] = next(iter(ids))
        wait("36 concurrent requests resolve to one transaction", lambda: payment_everywhere(next(iter(ids)), BASE))

        distributed = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            submissions = list(pool.map(lambda i: call(list(BASE)[i % 3], "/pay", "POST",
                key + "-distributed-" + str(i), 5 + i % 3), range(6)))
        assert all(status == 202 for status, _ in submissions), submissions
        distributed = [body["transactionId"] for _, body in submissions]
        evidence["concurrentDistributedTransactionIds"] = distributed
        completed = [wait("concurrent transaction completed on all nodes", lambda pid=pid: payment_everywhere(pid, BASE), 30)
                     for pid in distributed]
        assigned_nodes = {rows[0][1]["assignedNodeId"] for rows in completed}
        assert assigned_nodes == set(BASE), assigned_nodes
        evidence["workersObserved"] = sorted(assigned_nodes)
        print("PASS concurrent jobs executed by workers on all three nodes", flush=True)

        # Generate enough distinct confirmed entries to exercise DotNext snapshots.
        for i in range(12):
            assert call(old["nodeId"], "/pay", "POST", key + "-snapshot-" + str(i), 10)[0] == 202
        evidence["storageBefore"] = {n: compose("exec", "-T", n, "sh", "-c",
            "sha256sum /data/members.bin; find /data -type f -printf '%P %s bytes\\n'") for n in BASE}
        assert all("snapshots/" in text and "wal/" in text and "format.txt" in text for text in evidence["storageBefore"].values())
        print("PASS WAL, snapshots and format marker in all volumes", flush=True)

        compose("kill", old["nodeId"])
        survivors = [n for n in BASE if n != old["nodeId"]]
        replacement = wait("new leader after SIGKILL", lambda: cluster(survivors))
        assert replacement["nodeId"] != old["nodeId"] and replacement["term"] > old["term"]
        evidence["replacementLeader"] = replacement
        wait("payment survives leader failure", lambda: payment_everywhere(payment_id, survivors))
        assert call(survivors[0], "/pay", "POST", key, 10)[1]["transactionId"] == payment_id
        print("PASS retry after leader failure returns original ID", flush=True)
        other = next(n for n in survivors if n != replacement["nodeId"])
        compose("kill", other)
        isolated = replacement["nodeId"]
        def no_quorum():
            assert call(isolated, "/health/ready")[0] == 503
            assert call(isolated, "/pay", "POST", key + "-isolated", 7)[0] == 503
            assert call(isolated, "/payments/" + payment_id)[0] == 503
            return True
        wait("single node cannot confirm or serve definitive reads", no_quorum)
        compose("kill", isolated)
        compose("start")
        evidence["restartLeader"] = wait("election after full abrupt restart", lambda: cluster(BASE))
        evidence["restartPings"] = ping_everywhere(BASE)
        wait("transaction restored on all nodes", lambda: payment_everywhere(payment_id, BASE))
        assert all(call(n, "/pay", "POST", key, 10)[1]["transactionId"] == payment_id for n in BASE)
        evidence["storageAfter"] = {n: compose("exec", "-T", n, "sha256sum", "/data/members.bin") for n in BASE}
        assert all(evidence["storageBefore"][n].splitlines()[0] == evidence["storageAfter"][n].strip() for n in BASE)
        evidence["passed"] = True
        print("PASS persistence and membership after restart", flush=True)
    finally:
        try:
            compose("start")
            (OUT / "compose.log").write_text(compose("logs", "--no-color"), encoding="utf-8")
            evidence["containers"] = compose("ps", "--all")
        finally:
            (OUT / "result.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
