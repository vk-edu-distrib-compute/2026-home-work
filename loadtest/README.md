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
