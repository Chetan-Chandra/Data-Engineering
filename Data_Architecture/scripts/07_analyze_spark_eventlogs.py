
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import zstandard


def read_events(path):
    events = []

    with open(path, "rb") as compressed:
        reader = zstandard.ZstdDecompressor().stream_reader(compressed)

        import io
        text_reader = io.TextIOWrapper(reader, encoding="utf-8")

        for line in text_reader:
            line = line.strip()
            if line:
                events.append(json.loads(line))

    return events


def is_successful(event):
    reason = event.get("Task End Reason", {})
    if isinstance(reason, dict):
        return reason.get("Reason") == "Success"
    return reason == "Success"


def analyze(path):
    events = read_events(path)

    print("\n" + "=" * 75)
    print(f"FILE: {path}")
    print(f"TOTAL EVENTS: {len(events)}")
    print("=" * 75)

    event_counts = Counter(e.get("Event") for e in events)

    app_start = next(
        (e for e in events if e.get("Event") == "SparkListenerApplicationStart"),
        {}
    )
    app_end = next(
        (e for e in events if e.get("Event") == "SparkListenerApplicationEnd"),
        {}
    )

    print("APPLICATION:", app_start.get("App Name", "Unknown"))
    print("APPLICATION ID:", app_start.get("App ID", "Unknown"))
    print("START TIME:", app_start.get("Timestamp", "Unknown"))
    print("END TIME:", app_end.get("Timestamp", "Unknown"))

    print("\nEVENT COUNTS:")
    for name, count in event_counts.most_common():
        print(f"  {name}: {count}")

    stage_info = {}
    for event in events:
        if event.get("Event") == "SparkListenerStageCompleted":
            info = event.get("Stage Info", {})
            key = (info.get("Stage ID"), info.get("Stage Attempt ID", 0))
            stage_info[key] = info

    metrics = defaultdict(lambda: defaultdict(int))
    task_durations = defaultdict(list)
    attempts = defaultdict(int)
    successful = defaultdict(int)

    for event in events:
        if event.get("Event") != "SparkListenerTaskEnd":
            continue

        stage_id = event.get("Stage ID")
        stage_attempt = event.get("Stage Attempt ID", 0)
        key = (stage_id, stage_attempt)

        attempts[key] += 1

        if not is_successful(event):
            continue

        successful[key] += 1

        task = event.get("Task Info", {})
        launch_time = task.get("Launch Time", 0)
        finish_time = task.get("Finish Time", 0)

        if launch_time and finish_time:
            task_durations[key].append(finish_time - launch_time)

        tm = event.get("Task Metrics", {}) or {}

        metrics[key]["executor_run_time_ms"] += tm.get("Executor Run Time", 0)
        metrics[key]["executor_cpu_time_ns"] += tm.get("Executor CPU Time", 0)
        metrics[key]["jvm_gc_time_ms"] += tm.get("JVM GC Time", 0)
        metrics[key]["memory_spilled_bytes"] += tm.get("Memory Bytes Spilled", 0)
        metrics[key]["disk_spilled_bytes"] += tm.get("Disk Bytes Spilled", 0)

        shuffle_read = tm.get("Shuffle Read Metrics", {}) or {}
        metrics[key]["shuffle_read_bytes"] += (
            shuffle_read.get("Remote Bytes Read", 0)
            + shuffle_read.get("Local Bytes Read", 0)
        )
        metrics[key]["shuffle_read_records"] += shuffle_read.get("Records Read", 0)

        shuffle_write = tm.get("Shuffle Write Metrics", {}) or {}
        metrics[key]["shuffle_write_bytes"] += shuffle_write.get("Shuffle Bytes Written", 0)
        metrics[key]["shuffle_write_records"] += shuffle_write.get("Shuffle Records Written", 0)

        input_metrics = tm.get("Input Metrics", {}) or {}
        metrics[key]["input_bytes"] += input_metrics.get("Bytes Read", 0)
        metrics[key]["input_records"] += input_metrics.get("Records Read", 0)

        output_metrics = tm.get("Output Metrics", {}) or {}
        metrics[key]["output_bytes"] += output_metrics.get("Bytes Written", 0)
        metrics[key]["output_records"] += output_metrics.get("Records Written", 0)

    print("\nSTAGE / TASK METRICS:")

    for key in sorted(set(stage_info) | set(attempts)):
        info = stage_info.get(key, {})
        m = metrics[key]
        durations = task_durations[key]

        avg_duration = (
            sum(durations) / len(durations) if durations else 0
        )
        max_duration = max(durations) if durations else 0

        print(f"\nStage {key[0]}, Attempt {key[1]}")
        print("  Stage name:", info.get("Stage Name", "Unknown"))
        print("  Planned tasks:", info.get("Number of Tasks", "Unknown"))
        print("  Task attempts:", attempts[key])
        print("  Successful tasks:", successful[key])
        print(f"  Average task duration: {avg_duration:.2f} ms")
        print(f"  Maximum task duration: {max_duration} ms")
        print("  Executor run time:", m["executor_run_time_ms"], "ms")
        print("  JVM GC time:", m["jvm_gc_time_ms"], "ms")
        print("  Shuffle read:", m["shuffle_read_bytes"], "bytes")
        print("  Shuffle write:", m["shuffle_write_bytes"], "bytes")
        print("  Memory spill:", m["memory_spilled_bytes"], "bytes")
        print("  Disk spill:", m["disk_spilled_bytes"], "bytes")
        print("  Input records:", m["input_records"])
        print("  Output records:", m["output_records"])

    print("\nANALYSIS COMPLETE")


if __name__ == "__main__":
    for filename in sys.argv[1:]:
        analyze(filename)
