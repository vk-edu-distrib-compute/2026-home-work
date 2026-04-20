docker run --rm -i \
  -v "$PWD:/data" \
  peterevans/vegeta sh -c \
  "vegeta attack \
    -format=json \
    -targets=/data/get_targets.jsonl \
    -rate=200/s \
    -duration=30s \
    -workers=64 \
    -max-workers=128 \
    -connections=100 \
    -max-connections=100 \
    -keepalive \
    -http2=false \
  | tee /data/get_results_single.bin \
  | vegeta report -type=hdrplot > /data/get_single.hgrm"