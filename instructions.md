# Running the paper sweep experiments from a clean checkout

This guide assumes a clean clone of `shafinulh/flink-autoscaling` with submodules and describes the setup needed to run:

```bash
/opt/benchmark/query-runner-v2/run_paper_q7_sweep.sh
/opt/benchmark/query-runner-v2/run_paper_q9_sweep.sh
/opt/benchmark/query-runner-v2/run_paper_q20_sweep.sh
```

The scripts are written for the repo and runtime artifacts to live under `/opt`. Use `/opt` unless you are also prepared to update the hard-coded paths in `benchmark/query-runner-v2/run_query_runner_v2_sweep.sh`.

## 1. Clone the repo and submodules

```bash
sudo mkdir -p /opt
sudo chown "$USER":"$USER" /opt
git clone --recursive git@github.com:shafinulh/flink-autoscaling.git /opt
cd /opt
git submodule update --init --recursive
```

The submodules used by this flow are:

- `nexmark-v2`: Nexmark benchmark implementation and Kafka SQL connector source.
- `rocksdb-options`: custom Flink RocksDB options factory.
- `frocksdb` and `flink_1-20_src`: only needed when rebuilding a custom Flink/FrocksDB runtime.

## 2. Install host prerequisites

Install Java, Maven, build tools, SSH/rsync, and the small utilities used by the scripts:

```bash
sudo apt-get update
sudo apt-get install -y \
  openjdk-11-jdk maven build-essential curl tar rsync openssh-client \
  libsnappy-dev python3
```

If your Flink source build requires a different JDK, use the JDK required by that Flink checkout. The paper sweep scripts themselves do not require building Flink from source.

## 3. Configure local paths and cluster hosts

Create `scripts/env.local.sh` for machine-specific values. Use `export` for values that also need to be inherited by child scripts.

```bash
cat > /opt/scripts/env.local.sh <<'EOF'
export AUTOSCALING_ROOT=/opt
export FLINK_HOME=/opt/flink
export NEXMARK_HOME=/opt/nexmark

# Worker hosts that should receive /opt/flink and /opt/nexmark during sync.
# Leave empty for a single-node local run.
export WORKER_HOSTS="worker-hostname-or-ip"

# SSH user used by sync/log collection. Use root only if root SSH is how the
# cluster is configured.
export SSH_USER=root

# Hosts from which run_query_runner_v2_sweep.sh copies /data/rocksdb_native_logs.
# Usually the TaskManager host(s).
export LOG_HOSTS_STRING="worker-hostname-or-ip"
EOF
```

Before manually running scripts in a shell, source this file with auto-export enabled:

```bash
set -a
source /opt/scripts/env.sh
set +a
```

## 4. Stage a Flink runtime

For the paper sweeps, use the stable Flink runtime path unless you specifically need custom Flink source changes.

Default stable runtime:

```bash
cd /opt
sudo rm -rf /opt/flink
./scripts/setup_stable_flink_runtime.sh
```

`setup_stable_flink_runtime.sh` currently defaults to Apache Flink `1.20.3`. To force exact `1.20.0`, run:

```bash
FLINK_VERSION=1.20.0 ./scripts/setup_stable_flink_runtime.sh
```

Optional custom source runtime:

```bash
cd /opt
FLINK_SRC=/opt/flink_1-20_src ./scripts/build_flink_1-20_src.sh
sudo rm -rf /opt/flink
FLINK_SRC=/opt/flink_1-20_src ./scripts/setup_custom_flink_runtime.sh
```

Only use the custom path if you need the `flink_1-20_src` submodule changes. If you also need custom JNI changes from `frocksdb`, build/install those before building Flink.

## 5. Build and install `rocksdb-options`

This is required for `configs/flink/flink-conf-v2.yaml`. That config contains:

```yaml
state.backend.rocksdb.options-factory: com.example.CustomRocksDBOptionsFactory
```

Flink loads that class from jars in `${FLINK_HOME}/lib`. Build and copy it:

```bash
cd /opt
./scripts/build_rocksdb_options.sh
```

What the options factory does:

- Creates the RocksDB block cache and write buffer manager used by the state backend.
- Enables direct reads and direct IO for flush/compaction so cache misses measure real disk IO instead of the OS page cache.
- Registers RocksDB statistics and uses `/data/rocksdb_native_logs` for native RocksDB logs.
- Reads `state.backend.rocksdb.fixed-prefix-bytes` and `state.backend.rocksdb.bloom-filter.bits-per-key` from the active Flink config.
- Enables or disables fixed-prefix extraction and Bloom filters based on those values.

The paper sweep runner temporarily rewrites those two config keys for each experiment variant, then restores the config on exit.

## 6. Build and install Nexmark

Build the Nexmark package and the Kafka SQL connector, install Nexmark into `/opt/nexmark`, and copy the relevant jars into `/opt/flink/lib`:

```bash
cd /opt
./scripts/build_nexmark.sh clean
```

This uses:

- `NEXMARK_V2_SRC=/opt/nexmark-v2/nexmark-flink`
- `NEXMARK_KAFKA_CONNECTOR_SRC=/opt/nexmark-v2/flink-connector-kafka-3.3.0`

Override those in `scripts/env.local.sh` only if your checkout layout differs.

## 7. Configure the standalone cluster

Edit `/opt/configs/flink/flink-conf-v2.yaml` for your machines:

- `jobmanager.rpc.address`: set this to the JobManager hostname/IP.
- `state.checkpoints.dir` and `execution.checkpointing.savepoint-dir`: set these to writable paths.
- `state.backend.rocksdb.localdir`: ensure this exists on the TaskManager host, default `/data/rocksdb`.

Create the needed directories on the relevant hosts:

```bash
sudo mkdir -p /data/rocksdb /data/rocksdb_native_logs
sudo mkdir -p /mnt/experiments/nexmark-benchmark/flink-state/checkpoints
sudo mkdir -p /mnt/experiments/nexmark-benchmark/flink-state/savepoints
sudo chown -R "$USER":"$USER" /data/rocksdb /data/rocksdb_native_logs
sudo chown -R "$USER":"$USER" /mnt/experiments/nexmark-benchmark/flink-state
```

Update the Flink worker list after staging `/opt/flink`:

```bash
printf '%s\n' worker-hostname-or-ip > /opt/flink/conf/workers
```

Then apply the tracked Flink config and sync artifacts to workers:

```bash
cd /opt
./scripts/apply_flink_config.sh v2
./scripts/sync_cluster.sh
```

For a single-node local run, `WORKER_HOSTS` can be empty and `sync_cluster.sh` does nothing.

## 8. Run the paper sweeps

Do not manually start the Flink cluster before launching a sweep. `run_query_runner_v2_sweep.sh` starts and stops the cluster for each run.

Run all default `3g` and `8g` TaskManager memory sizes:

```bash
cd /opt
./benchmark/query-runner-v2/run_paper_q7_sweep.sh
./benchmark/query-runner-v2/run_paper_q9_sweep.sh
./benchmark/query-runner-v2/run_paper_q20_sweep.sh
```

Run only one memory size:

```bash
./benchmark/query-runner-v2/run_paper_q9_sweep.sh 8g
```

Useful environment overrides:

```bash
RUN_LABEL=paper-rerun-01 LOG_HOSTS_STRING="worker-hostname-or-ip" \
  ./benchmark/query-runner-v2/run_paper_q20_sweep.sh 8g
```

Outputs are written under `/opt/benchmark/query-runner-v2/<run-label>-.../`, with per-run metadata, copied configs, Nexmark config snapshots, run logs, profiling output, and RocksDB native logs.

## 9. What each paper script runs

Each script runs four variants:

1. `NM1 baseline`: original query, no filters.
2. `NM1 + filters`: original query, filters enabled.
3. `NM2 - filters`: unique-table query, no filters.
4. `NM2`: unique-table query, filters enabled.

Query event counts:

| Script | Queries | Warmup events | Eval events | Filtered fixed prefix | Filtered Bloom bits/key |
| --- | --- | ---: | ---: | ---: | ---: |
| `run_paper_q7_sweep.sh` | `q7`, `q7_unique` | 10,000,000 | 2,500,000 | 0 | 10.0 |
| `run_paper_q9_sweep.sh` | `q9`, `q9_unique` | 28,000,000 | 7,000,000 | 22 | 10.0 |
| `run_paper_q20_sweep.sh` | `q20`, `q20_unique` | 30,000,000 | 12,500,000 | 22 | 10.0 |

Q7 uses Bloom-only filtered runs because the paper artifact used `fixed-prefix=0,bloom=10.0` for Q7. Q9 and Q20 use `fixed-prefix=22,bloom=10.0`.

There is also a separate watermark-alignment experiment:

```bash
./benchmark/query-runner-v2/run_paper_q9_watermark_alignment_sweep.sh
```

It runs `q9_unique` with `100ms` and `1000s` watermark drift, `100000` TPS, `max-emit-speed=false`, `28M` warmup events, and `7M` eval events.

## 10. Common failure modes

- `ClassNotFoundException: com.example.CustomRocksDBOptionsFactory`: run `./scripts/build_rocksdb_options.sh` and make sure the jar is in `/opt/flink/lib` on every TaskManager.
- Cluster starts but workers do not connect: update `/opt/flink/conf/workers`, `jobmanager.rpc.address`, SSH access, and rerun `./scripts/sync_cluster.sh`.
- Savepoint/checkpoint errors: create or change the checkpoint/savepoint directories in `configs/flink/flink-conf-v2.yaml`.
- RocksDB log collection fails: set `LOG_HOSTS_STRING` to the TaskManager host(s) and make sure SSH works from the JobManager/control host.
- Existing `/opt/flink` or `/opt/nexmark`: the setup scripts are conservative. Remove the old runtime intentionally before rerunning setup.
