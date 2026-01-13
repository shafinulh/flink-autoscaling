#!/usr/bin/env python3
import argparse
import json
import csv
import re
from bisect import bisect_left
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

DETAIL_KEYS = [
    "compaction_reason",
    "output_level",
    "num_output_files",
    "total_output_size",
    "num_input_records",
    "num_output_records",
    "compaction_time_micros",
    "compaction_time_cpu_micros",
    "num_subcompactions",
    "input_data_size",
    "files_L0",
    "files_L1",
    "files_L2",
    "files_L3",
    "score",
    "flush_reason",
    "output_compression",
]

DATE_PREFIX_RE = re.compile(r"^\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2}\.\d+")
HIST_RE = re.compile(
    r"^(?P<name>rocksdb\.[^ ]+) P50 : (?P<p50>[-0-9.]+) "
    r"P95 : (?P<p95>[-0-9.]+) P99 : (?P<p99>[-0-9.]+) "
    r"P100 : (?P<p100>[-0-9.]+) COUNT : (?P<count>[-0-9.]+) SUM : (?P<sum>[-0-9.]+)"
)
COUNT_RE = re.compile(r"^(?P<name>rocksdb\.[^ ]+) COUNT : (?P<count>[-0-9.]+)")


def _normalize_levels(levels, max_levels):
    levels = list(levels)
    if len(levels) < max_levels:
        levels = levels + [0] * (max_levels - len(levels))
    elif len(levels) > max_levels:
        levels = levels[:max_levels]
    return levels


def _parse_timestamp(line):
    match = DATE_PREFIX_RE.match(line)
    if not match:
        return None
    ts = match.group(0)
    try:
        dt = datetime.strptime(ts, "%Y/%m/%d-%H:%M:%S.%f")
    except ValueError:
        return None
    return int(dt.timestamp() * 1_000_000)


def _parse_number(value):
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _parse_series_value(raw_value):
    if raw_value is None:
        return None
    text = str(raw_value).strip()
    if not text:
        return None
    if text.endswith("%"):
        try:
            return float(text[:-1])
        except ValueError:
            return None
    text = text.replace(",", "")
    text = text.replace("ops/s", "").strip()
    multiplier = 1.0
    if text and text[-1] in ("K", "M", "G"):
        suffix = text[-1]
        text = text[:-1]
        if suffix == "K":
            multiplier = 1_000.0
        elif suffix == "M":
            multiplier = 1_000_000.0
        elif suffix == "G":
            multiplier = 1_000_000_000.0
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def parse_csv_series(csv_path):
    points = []
    label = None
    if not csv_path:
        return {"label": "", "points": []}
    with open(csv_path, "r", errors="replace", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header and len(header) >= 2:
            label = header[1].strip('"')
        for row in reader:
            if len(row) < 2:
                continue
            time_text = row[0].strip()
            value_text = row[1].strip()
            try:
                dt = datetime.strptime(time_text, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            value = _parse_series_value(value_text)
            if value is None:
                continue
            points.append(
                {
                    "time_micros": int(dt.timestamp() * 1_000_000),
                    "value": value,
                }
            )
    points.sort(key=lambda item: item["time_micros"])
    return {"label": label or "", "points": points}


def parse_lsm(log_path, left_name, right_name, max_levels):
    job_to_cf = {}
    raw_events = []
    detected_max = 0

    with open(log_path, "r", errors="replace") as handle:
        for line in handle:
            if "EVENT_LOG_v1" not in line:
                continue
            idx = line.find("EVENT_LOG_v1")
            brace = line.find("{", idx)
            if brace == -1:
                continue
            json_str = line[brace:].strip()
            try:
                event = json.loads(json_str)
            except json.JSONDecodeError:
                continue

            job = event.get("job")
            cf_name = event.get("cf_name")
            if cf_name and job is not None:
                job_to_cf[job] = cf_name

            if "lsm_state" in event and isinstance(event["lsm_state"], list):
                detected_max = max(detected_max, len(event["lsm_state"]))

            raw_events.append(event)

    if max_levels is None:
        max_levels = detected_max or 7

    filtered = []
    for event in raw_events:
        if "lsm_state" not in event:
            continue
        time_micros = event.get("time_micros")
        if time_micros is None:
            continue
        if not isinstance(event["lsm_state"], list):
            continue

        job = event.get("job")
        cf_name = event.get("cf_name") or job_to_cf.get(job)
        if not cf_name:
            continue

        filtered.append(
            {
                "time_micros": time_micros,
                "event": event.get("event"),
                "job": job,
                "cf_name": cf_name,
                "lsm_state": _normalize_levels(event["lsm_state"], max_levels),
                "meta": {k: event.get(k) for k in DETAIL_KEYS if k in event},
            }
        )

    filtered.sort(key=lambda item: item["time_micros"])

    def zero_state():
        return [0] * max_levels

    state_by_cf = {}
    frames = []
    first_time = filtered[0]["time_micros"] if filtered else 0

    for event in filtered:
        cf_name = event["cf_name"]
        state_by_cf.setdefault(cf_name, zero_state())
        state_by_cf[cf_name] = list(event["lsm_state"])

        left_state = list(state_by_cf.get(left_name, zero_state()))
        right_state = list(state_by_cf.get(right_name, zero_state()))

        frames.append(
            {
                "time_micros": event["time_micros"],
                "t_rel_ms": (event["time_micros"] - first_time) / 1000.0,
                "event": event["event"],
                "job": event["job"],
                "cf_name": event["cf_name"],
                "lsm_state": event["lsm_state"],
                "left_state": left_state,
                "right_state": right_state,
                "meta": event["meta"],
            }
        )

    cf_names = sorted({item["cf_name"] for item in filtered})

    return {
        "meta": {
            "left_name": left_name,
            "right_name": right_name,
            "max_levels": max_levels,
            "frame_count": len(frames),
            "cf_names": cf_names,
        },
        "frames": frames,
    }

def parse_stats(log_path):
    dumps = []
    current = None
    first_time = None

    with open(log_path, "r", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if "STATISTICS:" in line:
                time_micros = _parse_timestamp(line.strip())
                current = {
                    "time_micros": time_micros,
                    "t_rel_ms": None,
                    "counters": {},
                    "histograms": {},
                }
                dumps.append(current)
                if time_micros is not None and first_time is None:
                    first_time = time_micros
                continue

            if current is None:
                continue

            stripped = line.strip()
            if not stripped:
                continue

            if DATE_PREFIX_RE.match(stripped):
                current = None
                continue

            if not stripped.startswith("rocksdb."):
                continue

            hist_match = HIST_RE.match(stripped)
            if hist_match:
                name = hist_match.group("name")
                current["histograms"][name] = {
                    "p50": _parse_number(hist_match.group("p50")),
                    "p95": _parse_number(hist_match.group("p95")),
                    "p99": _parse_number(hist_match.group("p99")),
                    "p100": _parse_number(hist_match.group("p100")),
                    "count": _parse_number(hist_match.group("count")),
                    "sum": _parse_number(hist_match.group("sum")),
                }
                continue

            count_match = COUNT_RE.match(stripped)
            if count_match:
                name = count_match.group("name")
                current["counters"][name] = _parse_number(count_match.group("count"))

    if first_time is None:
        first_time = dumps[0]["time_micros"] if dumps else None

    if first_time is not None:
        for dump in dumps:
            if dump["time_micros"] is None:
                dump["t_rel_ms"] = None
            else:
                dump["t_rel_ms"] = (dump["time_micros"] - first_time) / 1000.0

    return {
        "meta": {
            "dump_count": len(dumps),
        },
        "dumps": dumps,
    }


def find_csv_files(exp_dir):
    exp_path = Path(exp_dir)
    throughput = None
    hit_ratio = None
    for candidate in exp_path.glob("*.csv"):
        name = candidate.name
        if "throughput" in name:
            throughput = candidate
        elif "block_cache_hit_ratio" in name:
            hit_ratio = candidate
    return throughput, hit_ratio


def _nearest_lsm_index(times, target_time):
    if not times:
        return None
    idx = bisect_left(times, target_time)
    if idx == 0:
        return 0
    if idx >= len(times):
        return len(times) - 1
    before = times[idx - 1]
    after = times[idx]
    if target_time - before <= after - target_time:
        return idx - 1
    return idx


def build_markers(lsm_frames, stats_dumps, time_offset_micros):
    markers = []
    marker_events = {
        "flush_started": "Flush start",
        "flush_finished": "Flush end",
        "compaction_started": "Compaction start",
        "compaction_finished": "Compaction end",
    }
    lsm_times = [frame["time_micros"] for frame in lsm_frames]

    for index, frame in enumerate(lsm_frames):
        event = frame.get("event")
        if event not in marker_events:
            continue
        markers.append(
            {
                "time_micros": frame["time_micros"] + time_offset_micros,
                "event": event,
                "label": marker_events[event],
                "lsm_index": index,
            }
        )

    for dump in stats_dumps:
        time_micros = dump.get("time_micros")
        if time_micros is None:
            continue
        index = _nearest_lsm_index(lsm_times, time_micros)
        markers.append(
            {
                "time_micros": time_micros + time_offset_micros,
                "event": "stats_dump",
                "label": "Stats dump",
                "lsm_index": index,
            }
        )

    markers.sort(key=lambda item: item["time_micros"])
    return markers


def parse_experiment(log_path, left_name, right_name, max_levels):
    lsm = parse_lsm(log_path, left_name, right_name, max_levels)
    stats = parse_stats(log_path)
    return {
        "lsm": lsm,
        "stats": stats,
    }


def write_json(out_path, data):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle)
    return out_path


def find_experiment_logs(root_dir):
    root = Path(root_dir)
    experiments = []
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        candidates = sorted(child.glob("data_rocksdb*_db_LOG_*"))
        if not candidates:
            candidates = sorted(child.glob("data_rocksdb*LOG*"))
        if not candidates:
            continue
        experiments.append((child.name, candidates[0]))
    return experiments


def run_server(static_dir, host, port):
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(static_dir), **kwargs)

    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Serving {static_dir} on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        server.server_close()


def main():
    parser = argparse.ArgumentParser(
        description="Serve a timeline view of RocksDB LSM + statistics from LOG files."
    )
    parser.add_argument("--log", help="Path to a RocksDB LOG file")
    parser.add_argument(
        "--root",
        help="Root folder containing experiment subfolders with data_rocksdb* LOG files",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind")
    parser.add_argument(
        "--left-name",
        default="left-records",
        help="Column family name for the left panel",
    )
    parser.add_argument(
        "--right-name",
        default="right-records",
        help="Column family name for the right panel",
    )
    parser.add_argument(
        "--max-levels",
        type=int,
        default=None,
        help="Override max LSM levels (auto-detect if omitted)",
    )

    args = parser.parse_args()
    base_dir = Path(__file__).resolve().parent
    static_dir = base_dir / "static"

    if not args.log and not args.root:
        raise SystemExit("Provide either --log or --root.")

    if args.root:
        data_root = static_dir / "data"
        experiments = []
        for name, log_path in find_experiment_logs(args.root):
            data = parse_experiment(log_path, args.left_name, args.right_name, args.max_levels)
            throughput_csv, hit_ratio_csv = find_csv_files(log_path.parent)
            throughput = parse_csv_series(throughput_csv) if throughput_csv else None
            hit_ratio = parse_csv_series(hit_ratio_csv) if hit_ratio_csv else None

            series = {
                "throughput": throughput or {"label": "", "points": []},
                "block_cache_hit_ratio": hit_ratio or {"label": "", "points": []},
            }

            csv_times = []
            for series_points in (series["throughput"]["points"], series["block_cache_hit_ratio"]["points"]):
                if series_points:
                    csv_times.append(series_points[0]["time_micros"])
            csv_time_ref = min(csv_times) if csv_times else None
            log_time_ref = None
            if data["stats"]["dumps"]:
                log_time_ref = data["stats"]["dumps"][0].get("time_micros")
            if log_time_ref is None and data["lsm"]["frames"]:
                log_time_ref = data["lsm"]["frames"][0].get("time_micros")

            time_offset_micros = 0
            if csv_time_ref is not None and log_time_ref is not None:
                time_offset_micros = csv_time_ref - log_time_ref

            data["series"] = series
            data["markers"] = {
                "time_offset_micros": time_offset_micros,
                "items": build_markers(
                    data["lsm"]["frames"],
                    data["stats"]["dumps"],
                    time_offset_micros,
                ),
            }
            data["experiment"] = {
                "name": name,
                "log_path": str(log_path),
                "throughput_csv": str(throughput_csv) if throughput_csv else None,
                "hit_ratio_csv": str(hit_ratio_csv) if hit_ratio_csv else None,
            }
            out_path = data_root / f"{name}.json"
            write_json(out_path, data)
            experiments.append({"name": name, "file": f"data/{name}.json"})
            print(f"Wrote {out_path}")

        index_path = static_dir / "index.json"
        write_json(index_path, {"experiments": experiments})
        print(f"Wrote {index_path}")
    else:
        log_path = Path(args.log)
        if not log_path.exists():
            raise SystemExit(f"Log file not found: {log_path}")
        data = parse_experiment(log_path, args.left_name, args.right_name, args.max_levels)
        throughput_csv, hit_ratio_csv = find_csv_files(log_path.parent)
        throughput = parse_csv_series(throughput_csv) if throughput_csv else None
        hit_ratio = parse_csv_series(hit_ratio_csv) if hit_ratio_csv else None
        series = {
            "throughput": throughput or {"label": "", "points": []},
            "block_cache_hit_ratio": hit_ratio or {"label": "", "points": []},
        }
        csv_time_ref = None
        if series["throughput"]["points"]:
            csv_time_ref = series["throughput"]["points"][0]["time_micros"]
        elif series["block_cache_hit_ratio"]["points"]:
            csv_time_ref = series["block_cache_hit_ratio"]["points"][0]["time_micros"]

        log_time_ref = None
        if data["stats"]["dumps"]:
            log_time_ref = data["stats"]["dumps"][0].get("time_micros")
        if log_time_ref is None and data["lsm"]["frames"]:
            log_time_ref = data["lsm"]["frames"][0].get("time_micros")

        time_offset_micros = 0
        if csv_time_ref is not None and log_time_ref is not None:
            time_offset_micros = csv_time_ref - log_time_ref

        data["series"] = series
        data["markers"] = {
            "time_offset_micros": time_offset_micros,
            "items": build_markers(
                data["lsm"]["frames"],
                data["stats"]["dumps"],
                time_offset_micros,
            ),
        }
        data["experiment"] = {
            "name": log_path.stem,
            "log_path": str(log_path),
            "throughput_csv": str(throughput_csv) if throughput_csv else None,
            "hit_ratio_csv": str(hit_ratio_csv) if hit_ratio_csv else None,
        }
        data_path = write_json(static_dir / "data.json", data)
        print(f"Wrote {data_path}")

    run_server(static_dir, args.host, args.port)


if __name__ == "__main__":
    main()
