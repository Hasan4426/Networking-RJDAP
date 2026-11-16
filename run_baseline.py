import argparse
import csv
import json
import re
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


def read_log(path):
    return path.read_text(encoding="utf-8", errors="replace")


def stop_process(process):
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def verify_logs(folder):
    server_text = read_log(folder / "server.log")
    completed = re.findall(r"Task (\d+) completed: (-?\d+) \+ (-?\d+) = (-?\d+)", server_text)
    expected = list(range(1, 1001))
    if [int(row[0]) for row in completed] != expected:
        raise RuntimeError("Server must complete tasks 1..1000 exactly once and in order")
    if any(int(a) != int(task) or int(b) != 100 or int(answer) != int(a) + int(b)
           for task, a, b, answer in completed):
        raise RuntimeError("Incorrect task inputs or result")
    for worker_id in range(1, 5):
        text = read_log(folder / f"worker_{worker_id}.log")
        assigned = list(range(worker_id, 1001, 4))
        executions = re.findall(r"\| task (\d+): (-?\d+) \+ (-?\d+)", text)
        if [int(row[0]) for row in executions] != assigned:
            raise RuntimeError(f"Worker {worker_id}: missing, duplicate, or incorrectly assigned executions")
        if any(int(a) != int(task) or int(b) != 100 for task, a, b in executions):
            raise RuntimeError(f"Worker {worker_id}: incorrect inputs")
        acks = [int(task) for task in re.findall(r"\| task (\d+) acknowledged\.", text)]
        if acks != assigned or "Worker shutting down." not in text:
            raise RuntimeError(f"Worker {worker_id}: missing acknowledgments or shutdown")
    timing = re.search(r"Completed 1000/1000 tasks in ([0-9.]+) seconds", server_text)
    if timing is None:
        raise RuntimeError("Server completion timing is missing")
    return float(timing.group(1)) * 1000


def run_once(project, folder, number):
    folder.mkdir(parents=True)
    processes = []
    handles = []
    started = time.perf_counter()
    result = {"run": number, "status": "FAIL", "duration_ms": 0,
              "task_completion_ms": None, "reason": ""}

    def launch(filename, arguments, log_name):
        handle = (folder / log_name).open("w", encoding="utf-8")
        handles.append(handle)
        process = subprocess.Popen(
            [sys.executable, "-B", "-u", str(project / filename), *arguments],
            cwd=folder, stdout=handle, stderr=subprocess.STDOUT)
        processes.append(process)
        return process

    try:
        server = launch("server.py", [], "server.log")
        ready_deadline = time.monotonic() + 10
        while "Server listening on" not in read_log(folder / "server.log"):
            if server.poll() is not None:
                raise RuntimeError("Server exited before becoming ready; see server.log")
            if time.monotonic() >= ready_deadline:
                raise RuntimeError("Server startup timed out")
            time.sleep(0.05)
        for worker in range(1, 5):
            launch("worker.py", ["127.0.0.1", str(worker)], f"worker_{worker}.log")
        deadline = time.monotonic() + 180
        while any(process.poll() is None for process in processes):
            if any(process.poll() not in (None, 0) for process in processes):
                raise RuntimeError("Server or worker failed; inspect logs")
            if time.monotonic() >= deadline:
                raise RuntimeError("Baseline exceeded its 180-second time limit")
            time.sleep(0.05)
        if any(process.returncode != 0 for process in processes):
            raise RuntimeError("Server or worker failed; inspect logs")
        result["task_completion_ms"] = verify_logs(folder)
        result.update(status="PASS", reason="1000 correct tasks; 250 per worker; no duplicate executions; all ACKs and shutdowns verified")
    except (OSError, RuntimeError) as error:
        result["reason"] = str(error)
    finally:
        for process in reversed(processes):
            stop_process(process)
        for handle in handles:
            handle.close()
        result["duration_ms"] = round((time.perf_counter() - started) * 1000, 3)
        (folder / "result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--output", type=Path,
                        default=Path(__file__).resolve().parent / "baseline_logs")
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    project = args.project.resolve()
    for filename in ("Protocol.py", "server.py", "worker.py"):
        if not (project / filename).is_file():
            parser.error(f"Missing {project / filename}")

    root = args.output.resolve() / datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    root.mkdir(parents=True)
    results = []
    try:
        for number in range(1, args.runs + 1):
            result = run_once(project, root / f"run_{number:02d}", number)
            results.append(result)
            print(f"Run {number}/{args.runs}: {result['status']} - {result['reason']}", flush=True)
    except KeyboardInterrupt:
        print("Interrupted; child processes stopped. Completed runs will be summarized.")

    with (root / "results.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["run", "status", "duration_ms",
                                                  "task_completion_ms", "reason"])
        writer.writeheader()
        writer.writerows(results)
    passed = [row["task_completion_ms"] for row in results if row["status"] == "PASS"]
    success = len(passed) == args.runs
    summary = {
        "scenario": "Local baseline: four workers, 1000 sequential tasks, no injected impairment",
        "status": "PASS" if success else "FAIL", "requested_runs": args.runs,
        "completed_runs": len(results), "passed_runs": len(passed),
        "timing_definition": "task_completion_ms: server-reported task-loop duration (1 ms reporting precision); duration_ms: total process startup through shutdown",
        "successful_task_completion_ms": {
            "min": min(passed), "median": statistics.median(passed), "max": max(passed)
        } if passed else None,
        "notes": "No pcap capture. Log checks apply to the current server/worker output format."
    }
    (root / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Overall: {summary['status']} ({len(passed)}/{args.runs} passed)")
    if passed:
        print(f"Duration ms: min={min(passed):.3f}, median={statistics.median(passed):.3f}, max={max(passed):.3f}")
    print(f"Saved logs and results: {root}")
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
