# flink-autoscaling
Research on memory-aware autoscaling for stateful stream processing in Apache Flink. Investigating how RocksDB LSM-tree based storage and block cache performance affects Flink's stateful operator throughput and exploring alternatives to CPU-only scaling policies.

## Overview
This is experimental infrastructure for running Nexmark benchmarks on Flink with custom RocksDB configurations. The goal is to understand memory-throughput relationships for I/O-bound stateful operators and evaluate memory-aware autoscaling approaches.

Current focus:
- Profiling I/O-intensive Nexmark workloads to distinguish memory-bound vs. CPU-bound bottlenecks (previous autoscaling work evaluated queries with small state and low I/O utilization)
- Generating miss ratio curves (MRCs) to predict cache behavior across different memory allocations
- Comparing DS2 (CPU-based) vs Justin (memory-aware) autoscaling
- Building a memory-throughput model for RocksDB state backend that accounts for LSM-tree dynamics

## Layout
- `configs/flink/flink-conf.yaml`: Flink config used for experiments
- `scripts/`: repeatable workflows for building and running Nexmark on Flink cluster
- `benchmark/mrc-gen/`: Scripts to sweep memory configs for Nexmark query suite
- `benchmark/justin-autoscaler/`: Kubernetes YAML manifests for autoscaling experiments
- `nexmark-src/`: contains base and my own custom nexmark repo. currently working on optimizing the Nexmark SQL query definitions
-  `rocksdb-options/`: custom RocksDB options configurations for Flink jobs, which allows precise control over resource allocation
    - Manual read/write path memory configuration (block cache, write buffer manager sizing) 
    - Direcr reads/writes to bypass OS page cache and measure actual disk I/O
    - Enabling additional RocksDB micro-optimizations not exposed by default Flink API)
- `justin-custom-flink/`: customized Justin autoscaler build to enable support for running Nexmark SQL queries
- `monitoring/`: Prometheus + Grafana stack for profiling

## Common workflows
1) Build Nexmark Artifacts (base or custom):
```
./scripts/build_nexmark.sh base clean
./scripts/build_nexmark.sh custom clean
```

2) Build custom rocksdb-options:
```
./scripts/build_rocksdb_options.sh
```

3) Apply Flink config:
```
./scripts/apply_flink_config.sh
```

4) Sync Flink + Nexmark Artifacts to worker nodes:
```
./scripts/sync_cluster.sh
```

5) Start/stop a standalone cluster:
```
./scripts/start_cluster.sh
./scripts/stop_cluster.sh
```

6) Run a simple Nexmark query via Nexmark `run_query.sh`:
```
./scripts/run_nexmark_query.sh q1,q2,q3
```

7) Run the experimental SQL-query test flow (test for Justin experiments):
```
./scripts/run_nexmark_sql_query_test.sh --query q7 --duration-seconds 7200
```

## Configuration
default config in `scripts/env.sh` and private values in `scripts/env.local.sh`. e.g. `WORKER_HOSTS`, `FLINK_HOME`, `NEXMARK_HOME`, `JUSTIN_FLINK_HOME`.

## References
- **DS2**: [Three Steps is All You Need: Fast, Accurate, Automatic Scaling Decisions for Distributed Streaming Dataflows](https://www.usenix.org/conference/osdi18/presentation/kalavri) (OSDI '18)
- **Justin**: [Hybrid CPU/Memory Elastic Scaling for Distributed Stream Processing](https://link.springer.com/chapter/10.1007/978-3-031-50482-2_6) (DAIS '25) | [GitHub](https://github.com/CloudLargeScale-UCLouvain/flink-justin)