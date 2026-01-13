# RocksDB LSM Timeline Viewer

Small web UI to scrub through RocksDB LOG event data and see LSM level counts for two column families.

## Usage

Parse a whole experiment folder and start the server:

```bash
python3 /opt/rocksdb-flink-stats-viewer/app.py \
  --root /opt/benchmark/managed-mem-sensitivity/q20_unique-with_options \
  --port 8000
```

Parse a single LOG file:

```bash
python3 /opt/rocksdb-flink-stats-viewer/app.py \
  --log /opt/benchmark/managed-mem-sensitivity/q20_unique-with_options/exp-8g-tm-process/data_rocksdb_job_d9a343658910f8108739336c09681215_op_StreamingJoinOperator_29c6de9b0f6c5486908e9bb66a93ee45__1_1__uuid_e85660d7-e23b-443a-a4fb-c5689dc91aac_db_LOG_c182 \
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

## Notes

- The viewer uses `lsm_state` from RocksDB EVENT_LOG_v1 entries to show file counts per level.
- Stats dumps are parsed from `STATISTICS:` sections; counters use deltas between start/end dumps, histograms show the latest dump.
- If an experiment folder includes throughput and block cache hit ratio CSVs, the UI renders charts with flush/compaction/stats markers.
- Chart markers are time-aligned by comparing the first CSV timestamp with the first stats dump or LSM event.
- If your LOG does not include EVENT_LOG_v1 entries with `lsm_state`, the LSM timeline will be empty.
