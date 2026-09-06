# Kafka Canary

A Kafka availability and health canary.

Started by the Strimzi team as
[strimzi-canary](https://github.com/strimzi/strimzi-canary).
Upstream is archived; this repo continues that work.

![Go](https://img.shields.io/badge/Go-1.22.2-00ADD8?style=flat-square&logo=go&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-client-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-ready-2496ED?style=flat-square&logo=docker&logoColor=white)
[![GitHub release](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fapi.github.com%2Frepos%2Ftaekjaskyli%2Fkafka-canary%2Freleases%2Flatest&query=%24.tag_name&label=release&style=flat-square&color=blue)](https://github.com/taekjaskyli/kafka-canary/releases/latest)
[![License](https://img.shields.io/badge/License-Apache--2.0-22c55e?style=flat-square)](http://www.apache.org/licenses/LICENSE-2.0)

## What it does

Kafka Canary shows whether an [Apache Kafka](https://kafka.apache.org) cluster is actually usable, not just up.

It creates a dedicated canary topic, produces a message to every partition on a schedule, consumes those messages, and exports Prometheus metrics for produce latency, end-to-end latency, broker connectivity, and error counts.

Default `KAFKA_VERSION=3.2.0` works on Kafka 3.x and 4.x. That value is the protocol the client speaks; it does not have to match the broker version.

## Install

Image: `ghcr.io/taekjaskyli/kafka-canary`

Helm is the supported install path. Chart docs: [`deploy/helm/kafka-canary/README.md`](./deploy/helm/kafka-canary/README.md).

OCI:

```shell
helm install kafka-canary oci://ghcr.io/taekjaskyli/charts/kafka-canary \
  --version 0.8.0 \
  --set env.KAFKA_BOOTSTRAP_SERVERS=my-cluster-kafka-bootstrap:9092
```

Helm repository (`helm repo add`):

```shell
helm repo add kafka-canary https://taekjaskyli.github.io/kafka-canary
helm repo update
helm install kafka-canary kafka-canary/kafka-canary \
  --version 0.8.0 \
  --set env.KAFKA_BOOTSTRAP_SERVERS=my-cluster-kafka-bootstrap:9092
```

The chart always runs a single replica. Extra pods sharing the same consumer group would rebalance and make latency metrics noisy.

TLS and SASL credentials come from a Kubernetes Secret: mount files
(`volumes` / `volumeMounts` + path env vars) or `extraEnv` `secretKeyRef`.
Examples: [`deploy/helm/kafka-canary/README.md`](./deploy/helm/kafka-canary/README.md).

Enable a Prometheus Operator `ServiceMonitor` with `--set serviceMonitor.enabled=true`.

### Docker

```shell
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -p 8080:8080 \
  ghcr.io/taekjaskyli/kafka-canary:0.8.0
```

## Configuration

Environment variables. Omit a key to use the binary default. Some values can also be overridden at
runtime from a JSON file (`DYNAMIC_CONFIG_FILE`).

| Environment variable | Description | Default | Dynamic field |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Comma-separated bootstrap servers | `localhost:9092` | |
| `KAFKA_BOOTSTRAP_BACKOFF_MAX_ATTEMPTS` | Connect retries while the cluster is starting | `10` | |
| `KAFKA_BOOTSTRAP_BACKOFF_SCALE` | Delay scale between connect attempts (ms) | `5000` | |
| `TOPIC` | Canary topic | `kafka-canary` | |
| `TOPIC_CONFIG` | Topic config as `key=value` pairs separated by `;` | empty | |
| `RECONCILE_INTERVAL_MS` | Produce/consume interval (ms) | `30000` | |
| `CLIENT_ID` | Kafka `client.id` (broker logs, Prometheus `clientid` label). Not the consumer group. | `kafka-canary-client` | |
| `CONSUMER_GROUP_ID` | Kafka consumer group (`group.id`) | `kafka-canary-group` | |
| `PRODUCER_LATENCY_BUCKETS` | Histogram buckets for produce latency (ms) | `2,5,10,20,50,100,200,400` | |
| `ENDTOEND_LATENCY_BUCKETS` | Histogram buckets for end-to-end latency (ms) | `5,10,20,50,100,200,400,800` | |
| `EXPECTED_CLUSTER_SIZE` | Wait for this many brokers before creating the topic; `-1` = dynamic | `-1` | |
| `KAFKA_VERSION` | Kafka protocol the client speaks. Does not have to match the broker version. Default is fine for Kafka 3.x and 4.x. | `3.2.0` | |
| `SARAMA_LOG_ENABLED` | Enable Sarama client logs | `false` | `saramaLogEnabled` |
| `VERBOSITY_LOG_LEVEL` | `0` = INFO, `1` = DEBUG, `2` = TRACE | `0` | `verbosityLogLevel` |
| `TLS_ENABLED` | Use TLS | `false` | |
| `TLS_CA_CERT` | CA certificate: a filesystem path, or PEM in the env var value itself | empty | |
| `TLS_CLIENT_CERT` | Client certificate: a filesystem path, or PEM in the env var value itself | empty | |
| `TLS_CLIENT_KEY` | Client key: a filesystem path, or PEM in the env var value itself | empty | |
| `TLS_INSECURE_SKIP_VERIFY` | Skip broker TLS verify (not for production) | `false` | |
| `SASL_MECHANISM` | `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512` | empty | |
| `SASL_USER` | SASL username | empty | |
| `SASL_PASSWORD` | SASL password | empty | |
| `CONNECTION_CHECK_INTERVAL_MS` | Broker connection probe interval (ms) | `120000` | |
| `CONNECTION_CHECK_LATENCY_BUCKETS` | Histogram buckets for connection latency (ms) | `100,200,400,800,1600` | |
| `STATUS_CHECK_INTERVAL_MS` | How often `/status` samples are updated (ms) | `30000` | |
| `STATUS_TIME_WINDOW_MS` | Sliding window for consume success percentage (ms) | `300000` | |
| `DYNAMIC_CONFIG_FILE` | Optional JSON file watched for runtime overrides | empty | |
| `DYNAMIC_CONFIG_WATCHER_INTERVAL` | Config file poll interval (ms) | `30000` | |
| `EXPORTER_TYPE_TRACING` | Tracing exporter: empty (off), `jaeger`, or `otlp` | empty | |
| `PROMETHEUS_CONSTANT_LABELS` | Extra labels on all metrics, `key=value` pairs separated by `;` | empty | |

Runtime JSON example:

```json
{
  "saramaLogEnabled": true,
  "verbosityLogLevel": 1
}
```

On Kubernetes this file is typically a projected ConfigMap.

## HTTP endpoints

Listen on `:8080`.

| Path | Purpose |
|---|---|
| `/liveness` | Process is up (`OK`). Does not check Kafka. |
| `/readiness` | `503` until the first successful produce **and** consume, then `OK`. Stays `OK` after that so a later outage does not drop the pod from Service endpoints. |
| `/metrics` | Prometheus metrics |
| `/status` | JSON consume-success percentage over the sliding window |

`/status` example after the window has samples:

```json
{
  "Consuming": {
    "TimeWindow": 150000,
    "Percentage": 100
  }
}
```

Until the window has data, `Percentage` is `-1`.

## Metrics

Prometheus namespace is `kafka_canary`.

| Name | Description |
|---|---|
| `client_creation_error_total` | Errors creating the Sarama client |
| `expected_cluster_size_error_total` | Errors waiting for the expected broker count |
| `topic_creation_failed_total` | Errors creating the canary topic |
| `topic_describe_cluster_error_total` | Errors describing the cluster |
| `topic_describe_error_total` | Errors reading canary topic metadata |
| `topic_alter_assignments_error_total` | Errors altering partition assignments |
| `topic_alter_configuration_error_total` | Errors altering topic configuration |
| `records_produced_total` | Records produced |
| `records_produced_failed_total` | Produce failures |
| `producer_refresh_metadata_error_total` | Producer metadata refresh errors |
| `records_produced_latency` | Produce latency (ms) |
| `records_consumed_total` | Records consumed |
| `consumer_error_total` | Consumer errors |
| `consumer_timeout_join_group_total` | Consumer group join timeouts |
| `consumer_refresh_metadata_error_total` | Consumer metadata refresh errors |
| `records_consumed_latency` | End-to-end latency (ms) |
| `connection_error_total` | Deprecated; use `connection_total` |
| `connection_total` | Connection attempts (success or failure) |
| `connection_latency` | Connection latency (ms) |

Example Grafana dashboard: [`deploy/examples/metrics/grafana-dashboards/kafka-canary.json`](./deploy/examples/metrics/grafana-dashboards/kafka-canary.json).

With Prometheus Operator, enable the chart's ServiceMonitor (`serviceMonitor.enabled=true`) instead of applying a separate manifest.

## Migrating from Strimzi Canary

0.8.0 is a new product name. Defaults changed:

| | Upstream | kafka-canary 0.8.0 |
|---|---|---|
| Image | `quay.io/strimzi/canary` | `ghcr.io/taekjaskyli/kafka-canary` |
| Metrics | `strimzi_canary_*` | `kafka_canary_*` |
| Topic | `__strimzi_canary` | `kafka-canary` |
| Client id | `strimzi-canary-client` | `kafka-canary-client` |
| Group id | `strimzi-canary-group` | `kafka-canary-group` |

To keep the old topic and group, set `TOPIC`, `CLIENT_ID`, and `CONSUMER_GROUP_ID`. Dashboards must be updated for the new metric names (or import the dashboard in this repo).

## Build and contributing

Go **1.22.2**. See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Apache License 2.0. See [LICENSE](./LICENSE).

Original Canary code is Copyright Strimzi authors.
