# online-mrc

Benchmark scripts for generating block cache access traces and ground-truth
Miss Ratio Curves (MRCs) from a db\_bench Zipfian workload.

## Prerequisites

Build frocksdb first:

```bash
cd ../../frocksdb
make -j$(nproc) DEBUG_LEVEL=0 SNAPPY=1 db_bench block_cache_trace_analyzer
```

## Running the benchmark

### Plain run (stats only, no trace overhead)

```bash
./run_bench.sh --mode plain --run-id <name>
```

Output: `results/<name>/bench.log`

### Trace run (collect block cache trace)

```bash
./run_bench.sh --mode trace --run-id <name>
```

Output: `results/<name>/bench.log`, `results/<name>/trace.bin`

### Overriding parameters

Any variable from `env.sh` can be overridden inline:

```bash
CACHE_SIZE_MB=1024 ./run_bench.sh --mode plain --run-id test-1gb
NUM_KEYS=5000000 NUM_FILL_OPS=5000000 NUM_WORKLOAD_OPS=2500000 ./run_bench.sh --mode trace --run-id small-keys
ZIPF_ALPHA=0.99 ./run_bench.sh --mode trace --run-id high-alpha
KEY_DIST=uniform ./run_bench.sh --mode trace --run-id uniform-20m
```

Key parameters (see `env.sh` for all defaults):

| Variable | Default | Description |
|---|---|---|
| `KEY_DIST` | `zipfian` | `zipfian` or `uniform` |
| `ZIPF_ALPHA` | `0.6` | Zipfian shape; higher = more skewed |
| `NUM_KEYS` | `20000000` | Key space size |
| `NUM_FILL_OPS` | `20000000` | Keys written in fill phase |
| `NUM_WORKLOAD_OPS` | `10000000` | Ops in read/write phase |
| `CACHE_SIZE_MB` | `128` | Block cache size |
| `READ_WRITE_PERCENT` | `75` | % reads in workload phase |

## Generating the ground-truth MRC

After a trace run:

```bash
./gen_ground_truth_mrc.sh --run-id <name>
```

Output: `results/<name>/analysis/mrc.txt`

The script runs three steps:

1. Convert binary trace to human-readable CSV
2. Filter to user-level data-block accesses only (see below)
3. Simulate LRU caches across a range of sizes and record miss ratios

### What gets filtered and why

The ground-truth MRC reflects only the accesses that matter for user-facing
query performance. Two filters are applied:

**Block type = data blocks only (type 9)**

RocksDB records block cache accesses for several block types. Only data blocks
contain row data; the others are internal structures:

| Type | Description |
|---|---|
| 9 | Data block — row key/value pairs |
| 6 | Index block — points to data blocks |
| 7 | Filter block — bloom filter |
| 8 | Metadata block |

Index and filter blocks are typically tiny relative to data blocks and their
access pattern is structurally tied to compaction and open-file activity, not
to the query working set.

**Caller = user-level operations only (callers 1, 2, 3)**

RocksDB tags every block cache access with the caller that triggered it:

| Caller | Constant | Description |
|---|---|---|
| 1 | `kUserGet` | Point lookup (`Get`) |
| 2 | `kUserMultiGet` | Batch lookup (`MultiGet`) |
| 3 | `kUserIterator` | Scan / seek (`Iterator`) |
| 4 | `kCompaction` | Background compaction reads — **excluded** |

Compaction reads fetch large ranges of blocks sequentially and immediately
discard them; including them inflates the apparent working set and produces
an MRC that does not reflect interactive query behaviour.

After filtering, the MRC represents: *if I have a cache of size X, what
fraction of user-facing data-block lookups will miss?*

## Experiments in this repo

| Run ID | Key dist | Alpha | Keys | Cache | Notes |
|---|---|---|---|---|---|
| `test` | zipfian | 0.6 | 20M | 128 MB | plain, no trace |
| `test-trace` | zipfian | 0.6 | 20M | 128 MB | trace + MRC |
| `high-alpha` | zipfian | 0.99 | 20M | 128 MB | steeper hot set |
| `small-keys` | zipfian | 0.6 | 5M | 128 MB | 4x smaller key space |
| `uniform-20m` | uniform | — | 20M | 128 MB | no locality baseline |
