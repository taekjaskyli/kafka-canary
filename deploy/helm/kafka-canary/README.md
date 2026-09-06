# Kafka Canary Helm Chart

A Kafka availability and health canary. It creates a topic, produces and consumes
on a schedule, and exports Prometheus metrics.

This chart is part of [taekjaskyli/kafka-canary](https://github.com/taekjaskyli/kafka-canary),
which continues the archived [strimzi-canary](https://github.com/strimzi/strimzi-canary)
started by the Strimzi team.

## Requirements

- Helm 3.8+ (3.8 for OCI; 3.x for `helm repo add`)
- A reachable Kafka cluster

The canary is a Kafka client. It does not need a particular Kubernetes version
beyond whatever can run a single Deployment and Service.

The chart always runs **one replica**. Extra pods with the same consumer group
rebalance and make latency metrics noisy.

## Install from GHCR (OCI)

```bash
helm install kafka-canary oci://ghcr.io/taekjaskyli/charts/kafka-canary \
  --version 0.8.0 \
  --set env.KAFKA_BOOTSTRAP_SERVERS=my-cluster-kafka-bootstrap:9092 \
  --namespace kafka-canary \
  --create-namespace
```

Browse published versions: [ghcr.io/taekjaskyli/charts/kafka-canary](https://github.com/taekjaskyli/kafka-canary/pkgs/container/charts%2Fkafka-canary).

## Install from the Helm repository

Classic `helm repo add` (GitHub Pages, updated on each release):

```bash
helm repo add kafka-canary https://taekjaskyli.github.io/kafka-canary
helm repo update
helm install kafka-canary kafka-canary/kafka-canary \
  --version 0.8.0 \
  --set env.KAFKA_BOOTSTRAP_SERVERS=my-cluster-kafka-bootstrap:9092 \
  --namespace kafka-canary \
  --create-namespace
```

## Install from this repository

```bash
helm install kafka-canary deploy/helm/kafka-canary \
  --set env.KAFKA_BOOTSTRAP_SERVERS=my-cluster-kafka-bootstrap:9092 \
  --namespace kafka-canary \
  --create-namespace
```

## Configuration

Keys under `env` are the same environment variable names as
[project README Configuration](../../../README.md#configuration).
Leave a key out to use the binary default. Do not set empty strings.

```yaml
env:
  KAFKA_BOOTSTRAP_SERVERS: my-cluster-kafka-bootstrap:9092
  TOPIC: kafka-canary
  RECONCILE_INTERVAL_MS: 10000
```

## Kafka identities

These are three different Kafka concepts (all set via `env`):

| What | Env var | Default |
|---|---|---|
| Topic | `TOPIC` | `kafka-canary` |
| Consumer group (`group.id`) | `CONSUMER_GROUP_ID` | `kafka-canary-group` |
| Client id (`client.id`) | `CLIENT_ID` | `kafka-canary-client` |

`client.id` is what the broker logs and what Prometheus uses as the `clientid`
label. It is not the consumer group.

## Pre-created topic

When the canary principal must not create or alter the topic (`Describe` only,
no `AlterConfigs` / reassignment), set `MANAGE_TOPIC=false` and create the
topic yourself (typically partitions = brokers).

`EXPECTED_CLUSTER_SIZE` is not required for that. Omit it (default `-1`):
connection checks refresh the broker list each time. Set it to the broker
count only if you want probes pinned to that many brokers.

```yaml
env:
  KAFKA_BOOTSTRAP_SERVERS: my-cluster-kafka-bootstrap:9092
  MANAGE_TOPIC: false
```

## TLS

`TLS_CA_CERT`, `TLS_CLIENT_CERT`, and `TLS_CLIENT_KEY` accept a filesystem
path, or PEM as the env var value itself. Do not put certificates in Helm
values. Two common Kubernetes patterns:

**Path:** `volumes` + `volumeMounts`, then paths in `env`:

```yaml
env:
  TLS_ENABLED: true
  TLS_CA_CERT: /secrets/kafka-canary/ca.crt
  TLS_CLIENT_CERT: /secrets/kafka-canary/tls.crt
  TLS_CLIENT_KEY: /secrets/kafka-canary/tls.key
volumes:
  - name: tls
    secret:
      secretName: my-kafka-user
volumeMounts:
  - name: tls
    mountPath: /secrets/kafka-canary
    readOnly: true
```

**PEM in the env var:** `extraEnv` + `secretKeyRef` (bytes come from a Secret):

```yaml
env:
  TLS_ENABLED: true
extraEnv:
  - name: TLS_CA_CERT
    valueFrom:
      secretKeyRef:
        name: my-kafka-user
        key: ca.crt
  - name: TLS_CLIENT_CERT
    valueFrom:
      secretKeyRef:
        name: my-kafka-user
        key: tls.crt
  - name: TLS_CLIENT_KEY
    valueFrom:
      secretKeyRef:
        name: my-kafka-user
        key: tls.key
```

## SASL

Supported mechanisms: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`.

Set `env.SASL_MECHANISM` and take user/password from a Secret:

```yaml
env:
  SASL_MECHANISM: SCRAM-SHA-512
extraEnv:
  - name: SASL_USER
    valueFrom:
      secretKeyRef:
        name: my-kafka-user
        key: username
  - name: SASL_PASSWORD
    valueFrom:
      secretKeyRef:
        name: my-kafka-user
        key: password
```

`OAUTHBEARER` is OAuth 2.0 client credentials. Put the client secret in a Secret:

```yaml
env:
  SASL_MECHANISM: OAUTHBEARER
  SASL_OAUTH_TOKEN_URL: https://idp.example.com/realms/kafka/protocol/openid-connect/token
  SASL_OAUTH_CLIENT_ID: kafka-canary
extraEnv:
  - name: SASL_OAUTH_CLIENT_SECRET
    valueFrom:
      secretKeyRef:
        name: my-kafka-canary-oauth
        key: client-secret
```

## Sarama

Optional client timeouts, same style as `SARAMA_LOG_ENABLED`. Names: project README.
Applied at startup.

```yaml
env:
  SARAMA_PRODUCER_RETRY_MAX: 3
  SARAMA_CONSUMER_SESSION_TIMEOUT_MS: 45000
  SARAMA_CONSUMER_HEARTBEAT_INTERVAL_MS: 15000
```

## Prometheus

The pod serves `/metrics` on port `http` (8080).

With Prometheus Operator:

```bash
--set serviceMonitor.enabled=true
```

`podMonitor.enabled` is also available. Leave both false if you scrape via
annotations or a static Prometheus config.

Optional `PrometheusRule` (produce/consume failures and broker unreachable),
off by default. Same idea as ServiceMonitor: enable it, and set `labels`
if your Prometheus Operator instance selects rules by label.

```yaml
prometheusRule:
  enabled: true
```

Grafana dashboard JSON: [`deploy/examples/metrics/grafana-dashboards/kafka-canary.json`](../../examples/metrics/grafana-dashboards/kafka-canary.json).

## Upgrade

```bash
helm upgrade kafka-canary oci://ghcr.io/taekjaskyli/charts/kafka-canary \
  --version 0.8.0 \
  --namespace kafka-canary \
  -f my-values.yaml
```

Or, with the Helm repository:

```bash
helm repo update
helm upgrade kafka-canary kafka-canary/kafka-canary \
  --version 0.8.0 \
  --namespace kafka-canary \
  -f my-values.yaml
```

## Uninstall

```bash
helm uninstall kafka-canary --namespace kafka-canary
```

This does not delete the Kafka topic or consumer group. Remove those on the
brokers if you want them gone.

## Values

See [`values.yaml`](./values.yaml). Canary settings are the environment
variables in the [project README](../../../README.md#configuration), under `env`.
