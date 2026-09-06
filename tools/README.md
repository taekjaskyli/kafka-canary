# Canary latencies

Parses canary logs and prints average producer, end-to-end, and connection
latencies plus quantiles. Use it when tuning `PRODUCER_LATENCY_BUCKETS`,
`ENDTOEND_LATENCY_BUCKETS`, or `CONNECTION_CHECK_LATENCY_BUCKETS`.

Needs `VERBOSITY_LOG_LEVEL=1`. It reads log lines, not Prometheus metrics.

```shell
python3 canary_latencies.py -f canary.log
cat canary.log | python3 canary_latencies.py
python3 canary_latencies.py -f canary.log -c 10 -m inclusive
```
