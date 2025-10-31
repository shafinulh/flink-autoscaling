# Monitoring Nexmark Flink with Prometheus & Grafana

This folder contains example configuration to expose Flink metrics from the Nexmark workload to Prometheus and visualise them in Grafana.

## 1. Prometheus scrape configuration

File: `monitoring/prometheus/prometheus.yml`

1. Update the `static_configs.targets` list so it contains every metrics endpoint exported by your JobManager and TaskManagers.  
   * If several TaskManagers run on the same host, leave the port range wide enough in `flink-conf.yaml` and add one entry per port (for example `142.150.234.180:9249`, `142.150.234.180:9250`, …).  
   * The configuration sets `cluster=nexmark-flink` and extracts a `host` label from the address so you can filter per machine in Grafana.
2. Start Prometheus and point it at the config:

   ```bash
   docker run --rm -d \
     -p 9090:9090 \
     -v /opt/monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro \
     --name prometheus \
     prom/prometheus:v2.53.0
   ```

   Adjust the image tag or binary installation path if you already run Prometheus elsewhere. Use `promtool check config` before starting to validate edits.

## 2. Grafana provisioning

### Datasource

File: `monitoring/grafana/provisioning/datasources/prometheus.yml`

Provision this file under Grafana’s provisioning directory (default `/etc/grafana/provisioning/datasources/`). It creates a default Prometheus datasource called `Prometheus` that points to `http://prometheus:9090`. If your Prometheus runs on another host or port, change the URL before provisioning.

### Dashboard

Directory: `monitoring/grafana/provisioning/dashboards/` (contains `dashboard.yml` + `flink-overview.json`)

Provision the entire `monitoring/grafana/provisioning/` tree into `/etc/grafana/provisioning/`. Grafana loads `flink-overview.json` automatically thanks to `dashboard.yml`. The dashboard includes:

- TaskManager CPU load per host.
- Heap usage ratio per TaskManager.
- Last successful checkpoint duration per job (with a templated job selector).
- Output record rate per job.

The queries rely on the `cluster="nexmark-flink"` label and the `host` label added in the Prometheus config. Keep those labels consistent if you change the scrape configuration.

### Container-based setup

To spin up a minimal monitoring stack next to your Flink cluster:

Two quick start options:

1. **docker-compose (recommended)**  
   From `monitoring/`, run:

   ```bash
   docker compose up -d
   ```

   Compose uses `monitoring/docker-compose.yml` to spin up Prometheus (`prom/prometheus:v2.53.0`) and Grafana (`grafana/grafana-enterprise:10.4.2`), with persistent volumes `prometheus_data` and `grafana_data`. Dashboards and datasource configs are mounted from the repo.

2. **Manual docker run**

   ```bash
   docker network create monitoring

   docker run -d --name prometheus \
     --network monitoring \
     -p 9090:9090 \
     -v /opt/monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro \
     prom/prometheus:v2.53.0

   docker run -d --name grafana \
     --network monitoring \
     -p 3000:3000 \
     -v /opt/monitoring/grafana/provisioning:/etc/grafana/provisioning:ro \
     -v grafana-data:/var/lib/grafana \
     grafana/grafana-enterprise:10.4.2
   ```

   The named volume `grafana-data` keeps dashboard edits and alert rules across restarts (`docker volume create grafana-data` if needed). Remove the volume mount if you prefer Grafana to reset on every run.

Open `http://localhost:3000`, log in (default `admin` / `admin`), and confirm that the “Flink Nexmark Overview” dashboard renders. If your Prometheus runs outside the Docker network, either supply the correct hostname in `monitoring/grafana/provisioning/datasources/prometheus.yml` or map the host IP using `--add-host`.

## 3. Validation checklist

1. `curl http://<jm-host>:<port>/metrics` and `curl http://<tm-host>:<port>/metrics` both return metrics after the Flink cluster is up.  
2. In Prometheus, run `avg by (host) (flink_taskmanager_Status_JVM_CPU_Load)` to make sure targets are scraped successfully.  
3. In Grafana, the dashboard panels display non-empty time series while Nexmark queries are running. Use the job selector to focus on a single query if desired.

## 4. Operational tips

- If TaskManagers scale up/down dynamically, consider replacing the static target list with a file-based service discovery file (`file_sd_config`) generated from the Flink `workers` inventory.  
- The metrics endpoint listens on the first available port in the configured range. Keep the range narrow (for example `9249-9255`) so Prometheus discovery stays predictable.  
- Grafana provisioning loads dashboards on startup. After editing the JSON, restart Grafana or use the UI import to pick up the updates.  
- Restrict access to the metrics endpoints and Prometheus/Grafana UIs behind your preferred authentication mechanism when deploying in shared environments.
