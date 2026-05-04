# wrk2 Load Test

Run the service first:

```bash
./gradlew run
```

Then run the bonus load tests with wrk2 in one connection:

```bash
wrk -t1 -c1 -R200 -d30s --latency -s loadtest/put.lua http://localhost:8080 > loadtest/put.out
LOAD_FIRST_KEY=2 LOAD_KEYS=5999 wrk -t1 -c1 -R200 -d30s --latency -s loadtest/get.lua \
  "http://localhost:8080/v0/entity?id=load-2" > loadtest/get.out
```

With Docker on x86_64 Linux, mount this directory as `/data` and use the host address reachable from the container:

```bash
docker run --rm -v "$PWD/loadtest:/data" haydenjeune/wrk2 \
  -t1 -c1 -R200 -d30s --latency -s /data/put.lua http://host.docker.internal:8080 \
  > loadtest/put.out

docker run --rm -e LOAD_FIRST_KEY=2 -e LOAD_KEYS=5999 -v "$PWD/loadtest:/data" haydenjeune/wrk2 \
  -t1 -c1 -R200 -d30s --latency -s /data/get.lua "http://host.docker.internal:8080/v0/entity?id=load-2" \
  > loadtest/get.out
```

On Apple Silicon, the `haydenjeune/wrk2` amd64 image may crash under emulation. Build an arm64 wrk2 fork and run it in
an arm64 Alpine container:

```bash
rm -rf /tmp/wrk2-aarch64
git clone --depth 1 https://github.com/AmpereTravis/wrk2-aarch64.git /tmp/wrk2-aarch64

docker run --rm --platform linux/arm64 \
  -v /tmp/wrk2-aarch64:/src -w /src alpine:3.20 \
  sh -lc 'apk add --no-cache build-base openssl-dev zlib-dev perl linux-headers && make'

docker run --rm --platform linux/arm64 \
  -v /tmp/wrk2-aarch64:/wrk2 -v "$PWD/loadtest:/data" -w /wrk2 alpine:3.20 \
  sh -lc 'apk add --no-cache libgcc >/dev/null && ./wrk -t1 -c1 -R200 -d30s --latency \
    -s /data/put.lua http://host.docker.internal:8080' \
  > loadtest/put.out

docker run --rm --platform linux/arm64 \
  -v /tmp/wrk2-aarch64:/wrk2 -v "$PWD/loadtest:/data" -w /wrk2 alpine:3.20 \
  sh -lc 'apk add --no-cache libgcc >/dev/null && LOAD_FIRST_KEY=2 LOAD_KEYS=5999 ./wrk \
    -t1 -c1 -R200 -d30s --latency -s /data/get.lua \
    "http://host.docker.internal:8080/v0/entity?id=load-2"' \
  > loadtest/get.out
```

Upload the `Detailed Percentile spectrum` sections from `put.out` and `get.out` to
<https://hdrhistogram.github.io/HdrHistogram/plotFiles.html>, export PUT and GET images, and attach both images to
the pull request.

## Sharding bonus

Run the cluster first:

```bash
./gradlew run --args="cluster"
```

Run the cluster load test with the required parameters:

```bash
wrk -t2 -c100 -R200 -d30s --latency -s loadtest/request.lua http://localhost:8080 > loadtest/cluster.out
```

To compare with the monolith, stop the cluster, start the single-node service and run the same script:

```bash
./gradlew run
wrk -t2 -c100 -R200 -d30s --latency -s loadtest/request.lua http://localhost:8080 > loadtest/monolith.out
```

The distribution algorithm defaults to rendezvous hashing. To run the cluster with the bonus consistent-hash mode:

```bash
./gradlew run -Dce_fello.distribution=consistent --args="cluster"
```

## Replication

The replication-aware `ce_fello` service is controlled by two JVM properties:

```bash
-Dce_fello.replication.factor=3
-Dce_fello.replication.nodes=6
```

Start the service with the replication defaults:

```bash
./gradlew run -Dce_fello.replication.factor=3 -Dce_fello.replication.nodes=6
```

Run the quorum scenario (`ack_w + ack_r > n`, default `2 + 2 > 3`):

```bash
wrk -t2 -c100 -R200 -d30s --latency -s loadtest/replication_quorum.lua \
  http://localhost:8080 > loadtest/replication-quorum.out
```

Run the non-quorum scenario (`ack_w + ack_r <= n`, default `1 + 1 <= 3`):

```bash
wrk -t2 -c100 -R200 -d30s --latency -s loadtest/replication_non_quorum.lua \
  http://localhost:8080 > loadtest/replication-non-quorum.out
```

The scripts alternate `PUT` and `GET` on per-request keys and attach `ack` directly in the query string.
Use the resulting `Requests/sec`, percentile latency and `Non-2xx or 3xx responses` sections in the report. For
latency charts, upload the `Detailed Percentile spectrum` sections to
<https://hdrhistogram.github.io/HdrHistogram/plotFiles.html>.
