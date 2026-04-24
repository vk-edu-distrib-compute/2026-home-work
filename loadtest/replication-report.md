# Replication Load Test Report

Service configuration:

```bash
./gradlew run -Dce_fello.replication.factor=3 -Dce_fello.replication.nodes=6
```

wrk2 command template:

```bash
-t2 -c100 -R200 -d30s --latency
```

Scenarios:

1. Quorum: `ack_w=2`, `ack_r=2`, `ack_w + ack_r > n`
2. Non-quorum: `ack_w=1`, `ack_r=1`, `ack_w + ack_r <= n`

## Results

| Scenario | Throughput | Mean | p50 | p95 | p99 | Max | Error rate | Data actuality |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Quorum | 196.68 req/s | 4.73 ms | 4.11 ms | 9.34 ms | 11.66 ms | 21.34 ms | 0% | 100%* |
| Non-quorum | 200.04 req/s | 2.87 ms | 2.77 ms | 4.57 ms | 7.13 ms | 25.21 ms | 0% | 100%* |

\* `Data actuality` is inferred from the workload shape and output:
- each script alternates `PUT` and `GET` for the same key;
- both raw outputs contain no `Non-2xx or 3xx responses`;
- spot validation after the run returned the expected payload for a recorded quorum key.

## Analysis

- Quorum mode is slower across all central latency percentiles because each operation waits for more replica acknowledgements.
- Non-quorum mode stays almost exactly at the target rate `200 req/s`; quorum mode drops slightly to `196.68 req/s`.
- The delta is most visible in read/write coordination cost, not in catastrophic failures: both scenarios completed without HTTP errors.
- Under this healthy-node workload both scenarios returned up-to-date values, but non-quorum gives weaker guarantees under replica loss or delayed propagation.

## Practical conclusion

- `ack=2` for reads and writes is the safer production default for `n=3` when consistency matters.
- `ack=1` is cheaper and faster, but it trades latency for weaker consistency guarantees under failures.
- The current bottleneck is replication fan-out and confirmation waiting, so further production tuning should focus on replica I/O path, timeout tuning and reducing per-request coordination overhead.
