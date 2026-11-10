"""Increment 01 integration gate. Python 3 stdlib + Docker Compose; run from the repo root."""
import argparse
import json
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
URLS = {"node-a": "http://127.0.0.1:5001", "node-b": "http://127.0.0.1:5002", "node-c": "http://127.0.0.1:5003"}
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def compose(*args):
    result = subprocess.run(["docker", "compose", *args], cwd=ROOT,
                            text=True, encoding="utf-8", capture_output=True, timeout=300)
    if result.returncode:
        raise RuntimeError(f"docker compose {' '.join(args)} failed:\n{result.stdout}\n{result.stderr}")
    return result.stdout


def request(node, path, method="GET"):
    req = urllib.request.Request(URLS[node] + path, method=method)
    try:
        with HTTP.open(req, timeout=6) as response:
            data = response.read()
            return response.status, json.loads(data) if data else None
    except urllib.error.HTTPError as error:
        data = error.read()
        return error.code, json.loads(data) if data else None


def wait_for(description, check, seconds=60):
    deadline = time.monotonic() + seconds
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                print("PASS: " + description, flush=True)
                return result
        except (AssertionError, OSError, urllib.error.URLError) as error:
            last_error = str(error)
        time.sleep(0.5)
    raise AssertionError(f"Timed out: {description}. Last error: {last_error}")


def cluster(nodes):
    states = [request(node, "/cluster")[1] for node in nodes]
    leaders = [state for state in states if state["role"] == "leader"]
    assert len(leaders) == 1, states
    leader = leaders[0]
    for state in states:
        assert state["ready"] and state["quorum"] == "confirmed", states
        assert state["leaderId"] == leader["nodeId"], states
        assert state["term"] == leader["term"], states
        assert set(state["members"]) == set(URLS), states
    return leader


def stable_cluster(nodes):
    first = cluster(nodes)
    time.sleep(0.4)
    second = cluster(nodes)
    assert (first["nodeId"], first["term"]) == (second["nodeId"], second["term"])
    return second


def ping_all(nodes):
    count = 0
    for sender in nodes:
        for receiver in nodes:
            if sender == receiver:
                continue
            status, pong = request(sender, "/diagnostics/ping/" + receiver, "POST")
            assert status == 200 and pong == {"senderId": sender, "receiverId": receiver, "transport": "IMessageBus"}, (status, pong)
            count += 1
    return count


def assert_values(nodes, expected):
    for node in nodes:
        status, body = request(node, "/diagnostics/value")
        assert status == 200 and body["value"] == expected, (node, status, body)
    return True


def storage_evidence():
    return {node: compose("exec", "-T", node, "sh", "-c",
                         "sha256sum /data/members.bin; find /data -type f -printf '%P %s bytes\\n'")
            for node in URLS}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    evidence = {"startedUtc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "passed": False}
    artifacts = ROOT / ".artifacts" / "integration"
    artifacts.mkdir(parents=True, exist_ok=True)
    try:
        compose("up", "-d", *([] if args.skip_build else ["--build"]))
        leader = wait_for("three nodes agree on one leader", lambda: stable_cluster(URLS))
        evidence["initialLeader"] = leader
        evidence["initialPings"] = ping_all(URLS)
        print("PASS: six directed IMessageBus ping/pong exchanges", flush=True)
        for node in URLS:
            assert request(node, "/health/live")[0] == 200
            assert request(node, "/health/ready")[0] == 200
            assert request(node, "/cluster-consensus/raft", "POST")[0] == 404
        print("PASS: health endpoints and private Raft transport", flush=True)

        value = int(time.time())
        for offset in range(12):
            status, _ = request(leader["nodeId"], f"/diagnostics/value/{value + offset}", "POST")
            assert status == 200, status
        value += 11
        wait_for("diagnostic WAL value replicated to all nodes", lambda: assert_values(URLS, value))
        evidence["storageBefore"] = storage_evidence()

        compose("kill", leader["nodeId"])
        survivors = [node for node in URLS if node != leader["nodeId"]]
        new_leader = wait_for("new leader after abrupt leader failure", lambda: stable_cluster(survivors))
        assert new_leader["nodeId"] != leader["nodeId"] and new_leader["term"] > leader["term"]
        evidence["replacementLeader"] = new_leader
        evidence["survivorPings"] = ping_all(survivors)
        value += 1
        assert request(new_leader["nodeId"], f"/diagnostics/value/{value}", "POST")[0] == 200
        wait_for("two nodes still commit and apply", lambda: assert_values(survivors, value))

        other = next(node for node in survivors if node != new_leader["nodeId"])
        compose("kill", other)
        isolated = new_leader["nodeId"]

        def no_quorum():
            status, state = request(isolated, "/health/ready")
            assert status == 503 and not state["ready"] and state["quorum"] == "unknown", (status, state)
            assert set(state["members"]) == set(URLS)
            assert request(isolated, "/health/live")[0] == 200
            assert request(isolated, f"/diagnostics/value/{value + 1000}", "POST")[0] == 503
            return state

        evidence["withoutQuorum"] = wait_for("one node is alive but not ready and cannot confirm", no_quorum)
        compose("kill", isolated)
        compose("start")
        evidence["restartedLeader"] = wait_for("cluster reforms from the existing volumes", lambda: stable_cluster(URLS))
        wait_for("committed diagnostic value survives full abrupt restart", lambda: assert_values(URLS, value))
        evidence["restartedPings"] = ping_all(URLS)
        evidence["storageAfter"] = storage_evidence()
        for node in URLS:
            assert evidence["storageBefore"][node].splitlines()[0] == evidence["storageAfter"][node].splitlines()[0]
        evidence["persistedValue"] = value
        evidence["passed"] = True
        print("PASS: fixed membership persisted unchanged; increment 01 gate complete", flush=True)
    finally:
        # Leave all three services running and preserve every volume, including on failure.
        try:
            compose("start")
            (artifacts / "compose.log").write_text(compose("logs", "--no-color"), encoding="utf-8")
            evidence["containers"] = compose("ps", "--all")
        finally:
            (artifacts / "result.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
