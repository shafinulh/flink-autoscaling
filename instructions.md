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

## 2. Install host prerequisites

Install Java, Maven, build tools, SSH/rsync, and the small utilities used by the scripts:

```bash
sudo apt-get update
sudo apt-get install -y \
  openjdk-11-jdk maven build-essential curl tar rsync openssh-client \
  python3
```

The paper sweep scripts do not require building Flink from source.

## 3. Configure local paths and cluster hosts

Create `scripts/env.local.sh` for machine-specific values. Use `export` for values that also need to be inherited by child scripts.

```bash
cat > /opt/scripts/env.local.sh <<'EOF'
export AUTOSCALING_ROOT=/opt
export FLINK_HOME=/opt/flink
export NEXMARK_HOME=/opt/nexmark

# Current two-node setup:
# - c180 / 142.150.234.180: JobManager and control node
# - c155 / 142.150.234.155: TaskManager worker
export WORKER_HOSTS="142.150.234.155"

# SSH user used by sync/log collection. Use an account that can write /opt on
# c155; use root only if that is how the cluster is configured.
export SSH_USER=haques24

# Hosts from which run_query_runner_v2_sweep.sh copies /data/rocksdb_native_logs.
# Usually the TaskManager host(s).
export LOG_HOSTS_STRING="142.150.234.155"
EOF
```

Before manually running scripts in a shell, source this file with auto-export enabled:

```bash
set -a
source /opt/scripts/env.sh
set +a
```

## 4. Stage a Flink runtime

Use the stable Flink runtime path for the paper sweeps:

```bash
cd /opt
sudo rm -rf /opt/flink
./scripts/setup_stable_flink_runtime.sh
```

`setup_stable_flink_runtime.sh` currently defaults to Apache Flink `1.20.3`. To force exact `1.20.0`:

```bash
FLINK_VERSION=1.20.0 ./scripts/setup_stable_flink_runtime.sh
```

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

- Configures the RocksDB block cache/write buffer manager used by the state backend.
- Enables RocksDB metrics and native stats dumps.
- Reads `state.backend.rocksdb.fixed-prefix-bytes` and `state.backend.rocksdb.bloom-filter.bits-per-key` from the active Flink config.

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

- `jobmanager.rpc.address`: for the current cluster, use c180's IP:
  `142.150.234.180`.
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

Update the Flink worker list on c180 after staging `/opt/flink`. Today the single worker is c155:

```bash
printf '%s\n' 142.150.234.155 > /opt/flink/conf/workers
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

Before rerunning after an interrupted or failed sweep, make sure the previous cluster is down:

```bash
cd /opt
./scripts/stop_cluster.sh
```

That script runs Flink shutdown and Nexmark shutdown. If the sweep is still running in your terminal, stop it with `Ctrl-C` and wait for the cleanup messages to finish before running `stop_cluster.sh` manually.

If you still see lingering Flink components, run the explicit shutdown commands from c180:

```bash
/opt/flink/bin/stop-cluster.sh || true
/opt/flink/bin/stop-cluster.sh || true
/opt/nexmark/bin/shutdown_cluster.sh || true
ssh haques24@142.150.234.155 '/opt/flink/bin/taskmanager.sh stop || true'
```

Then verify with `jps` or `ps -ef | grep -E 'StandaloneSessionClusterEntrypoint|TaskManagerRunner|run_query'` on c180 and c155 before restarting a sweep.

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

RocksDB native logs are important because the options factory writes periodic metrics dumps there. On c155 the source path is:

```text
/data/rocksdb_native_logs
```

On a completed run, `run_query_runner_v2_sweep.sh` copies that directory into the run output under `rocksdb_logs/` and then deletes the remote files. On an incomplete run, copy or delete them manually before restarting so the next run does not mix metrics dumps:

```bash
mkdir -p /opt/benchmark/query-runner-v2/manual-rocksdb-logs
scp -r haques24@142.150.234.155:/data/rocksdb_native_logs \
  /opt/benchmark/query-runner-v2/manual-rocksdb-logs/rocksdb_native_logs-$(date +%Y%m%d-%H%M%S)

ssh haques24@142.150.234.155 \
  'find /data/rocksdb_native_logs -mindepth 1 -maxdepth 1 -exec rm -rf {} +'
```

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
