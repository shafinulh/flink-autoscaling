# RocksDB LSM Timeline Viewer v2

Small web UI to scrub through RocksDB LOG event data and see LSM level counts for two column families.

## Usage

Parse a whole experiment folder and start the server (Prometheus-backed time series):

```bash
python3 /opt/rocksdb-flink-stats-viewer-v2/app.py \
  --root /opt/benchmark/managed-mem-sensitivity/q20_unique-with_options \
  --prometheus-url http://localhost:9090 \
  --promql-throughput '<PROMQL_FOR_THROUGHPUT>' \
  --promql-hit-ratio '<PROMQL_FOR_BLOCK_CACHE_HIT_RATIO>' \
  --job-regex 'q20_unique_p1' \
  --series-offset-hours -5 \
  --port 8000
```

Parse a single LOG file:

```bash
python3 /opt/rocksdb-flink-stats-viewer-v2/app.py \
  --log /opt/benchmark/managed-mem-sensitivity/q20_unique-with_options/exp-8g-tm-process/data_rocksdb_job_d9a343658910f8108739336c09681215_op_StreamingJoinOperator_29c6de9b0f6c5486908e9bb66a93ee45__1_1__uuid_e85660d7-e23b-443a-a4fb-c5689dc91aac_db_LOG_c182 \
  --prometheus-url http://localhost:9090 \
  --promql-throughput '<PROMQL_FOR_THROUGHPUT>' \
  --promql-hit-ratio '<PROMQL_FOR_BLOCK_CACHE_HIT_RATIO>' \
  --job-regex 'q20_unique_p1' \
  --series-offset-hours -5 \
  --port 8000
```

Then browse:

```
http://<host>:8000
```

If you are on a remote machine, use port forwarding:

```bash
ssh -L 8000:localhost:8000 <user>@<host>
```

Then open:

```
http://localhost:8000
```

## Options

- `--left-name` and `--right-name`: column family names to map to the two panels.
- `--max-levels`: override number of LSM levels (auto-detect if omitted).
- `--root`: parse all experiment subfolders with `data_rocksdb*` LOG files.
- `--prometheus-url`: Prometheus base URL for time series data.
- `--promql-throughput`: PromQL query for source throughput.
- `--promql-hit-ratio`: PromQL query for block cache hit ratio.
- `--promql-step`: step in seconds for query range (default 10).
- `--job-regex`: regex injected into default PromQL `$job` placeholder.
- `--series-offset-hours`: shift RocksDB timestamps to align with monitoring data.

## Notes

- The viewer uses `lsm_state` from RocksDB EVENT_LOG_v1 entries to show file counts per level.
- Stats dumps are parsed from `STATISTICS:` sections; counters use deltas between start/end dumps, histograms show the latest dump.
- Time series data is fetched from Prometheus. If Prometheus is not reachable, the app falls back to CSVs in the experiment folder.
- Chart markers are time-aligned by comparing the first series timestamp with the first stats dump or LSM event (or by `--series-offset-hours`).
- If your LOG does not include EVENT_LOG_v1 entries with `lsm_state`, the LSM timeline will be empty.
- Default PromQLs are pulled from the provisioned Grafana dashboards in `/opt/monitoring/grafana/provisioning/dashboards/`.
- You can add custom PromQL plots in the UI; they are queried through the app using the stats time window plus the configured offset.
- When an experiment folder contains multiple RocksDB LOG files, the UI shows an operator selector and defaults to StreamingJoinOperator when available.
