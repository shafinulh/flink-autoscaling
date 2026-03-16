# offline-mrc-gen

Copy the raw trace for the query into:

```text
flink-autoscaling/benchmark/offline-mrc-gen/inputs/q20u_no_filters/trace.trace
```

Ground-truth files for that query go next to it, for example:

```text
flink-autoscaling/benchmark/offline-mrc-gen/inputs/q20u_no_filters/ground_truth_mrc.txt
flink-autoscaling/benchmark/offline-mrc-gen/inputs/q20u_no_filters/ground_truth_mrc_data_only.txt
flink-autoscaling/benchmark/offline-mrc-gen/inputs/q20u_no_filters/ground_truth_mrc_data_user_only.txt
```

Set local tool paths in:

```text
flink-autoscaling/benchmark/offline-mrc-gen/env.local.sh
```

The main one you need is `BLOCK_CACHE_TRACE_ANALYZER`.

Prepare traces

```bash
cd flink-autoscaling/benchmark/offline-mrc-gen
./prepare_traces.sh
```

This creates:

```text
flink-autoscaling/benchmark/offline-mrc-gen/outputs/q20u_no_filters/human.txt
flink-autoscaling/benchmark/offline-mrc-gen/outputs/q20u_no_filters/data_only_human.txt
flink-autoscaling/benchmark/offline-mrc-gen/outputs/q20u_no_filters/data_user_only_human.txt
```

Cleaning up traces

- `data_only_human.txt` keeps only RocksDB data block accesses: `block_type == 9`.
- `data_user_only_human.txt` further keeps only user foreground accesses. callers `1`, `2`, `3` (`kUserGet`, `kUserMultiGet`, `kUserIterator`).
- We drop compaction/background accesses because they are not inserted into the block cache, and we do not care about compaction miss rate as it occurs in the background.

If you want to see the filtering directly, it is:

```bash
OUT=flink-autoscaling/benchmark/offline-mrc-gen/outputs/q20u_no_filters

awk -F',' '$3 == 9' \
  "$OUT/human.txt" \
  > "$OUT/data_only_human.txt"

awk -F',' '$3 == 9 && ($9 == 1 || $9 == 2 || $9 == 3)' \
  "$OUT/human.txt" \
  > "$OUT/data_user_only_human.txt"
```

Run SHARDS

`run_shards.sh` takes:

- `--human-trace`: which prepared human-readable trace to use
- `--ground-truth`: which ground-truth MRC to compare against
- `--sampling`: SHARDS sampling value
- `--run-id`: output name for bins and plots

Example commands:

```bash
cd flink-autoscaling/benchmark/offline-mrc-gen

./run_shards.sh \
  --human-trace outputs/q20u_no_filters/human.txt \
  --ground-truth inputs/q20u_no_filters/ground_truth_mrc.txt \
  --sampling 0.01 \
  --run-id shards

./run_shards.sh \
  --human-trace outputs/q20u_no_filters/data_only_human.txt \
  --ground-truth inputs/q20u_no_filters/ground_truth_mrc_data_only.txt \
  --sampling 0.01 \
  --run-id data_only_shards_s0p01

./run_shards.sh \
  --human-trace outputs/q20u_no_filters/data_only_human.txt \
  --ground-truth inputs/q20u_no_filters/ground_truth_mrc_data_only.txt \
  --sampling 1.0 \
  --run-id data_only_shards_no_sampling

./run_shards.sh \
  --human-trace outputs/q20u_no_filters/data_user_only_human.txt \
  --ground-truth inputs/q20u_no_filters/ground_truth_mrc_data_user_only.txt \
  --sampling 0.01 \
  --run-id data_user_only_shards_s0p01

./run_shards.sh \
  --human-trace outputs/q20u_no_filters/data_user_only_human.txt \
  --ground-truth inputs/q20u_no_filters/ground_truth_mrc_data_user_only.txt \
  --sampling 1.0 \
  --run-id data_user_only_shards_no_sampling
```

Outputs

- SHARDS bins go under `flink-autoscaling/benchmark/offline-mrc-gen/outputs/q20u_no_filters/`
- MRC plots go under `flink-autoscaling/benchmark/offline-mrc-gen/plots/q20u_no_filters/mrc/`
- SHARDS-vs-ground-truth plots go under `flink-autoscaling/benchmark/offline-mrc-gen/plots/q20u_no_filters/mrc_vs_ground_truth/`
