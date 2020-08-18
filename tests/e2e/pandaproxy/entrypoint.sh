#!/bin/bash
set -e
set -x

BROKERS=${2:-172.16.5.1:9092}
NAMESPACE=${4:-test-topic}
CONF=/opt/pandaproxy/conf/pandaproxy.yaml

function fixup_conf() {
  # Only one broker supported
  readarray -td : -t BROKER_SPLIT < <(printf '%s' "$BROKERS")

  sed -i 's/127.0.0.1/'"${BROKER_SPLIT[0]}"'/' $CONF
  sed -i 's/9092/'"${BROKER_SPLIT[1]}"'/' $CONF
  sed -i 's|/etc/pandaproxy|/opt/pandaproxy/etc/pandaproxy|' $CONF
}

function cleanup() {
  /opt/pandaproxy/bin/rpk api topic delete "$NAMESPACE-0" --brokers "$BROKERS" || true
  /opt/pandaproxy/bin/rpk api topic delete "$NAMESPACE-1" --brokers "$BROKERS" || true
}
trap cleanup EXIT

function wait_for_rest() {
  timeout "${1:-10}" bash -c 'while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' localhost:8082/topics)" != "200" ]]; do sleep 1; done' || false
}

# Start Pandaproxy
fixup_conf
/opt/pandaproxy/bin/pandaproxy --pandaproxy-cfg $CONF --smp=1 --default-log-level=trace &

# Create test topics
/opt/pandaproxy/bin/rpk api topic create "$NAMESPACE-0" --brokers "$BROKERS"
/opt/pandaproxy/bin/rpk api topic create "$NAMESPACE-1" --brokers "$BROKERS"

wait_for_rest 10

## Tests ##

# Validate Swagger

curl -s http://127.0.0.1:8082/v1 -o swagger20.json

## Test 'get_topics_names' exists
jq -e '.paths["/topics"]["get"]["operationId"] == "get_topics_names"' swagger20.json

## Test 'post_topics_name' exists
jq -e '.paths["/topics/{topic_name}"]["post"]["operationId"] == "post_topics_name"' swagger20.json

## Test overall validity
swagger-cli validate swagger20.json

# Validate Content-Type
curl -s -D - -o /dev/null http://127.0.0.1:8082/topics | tr -d '\r' | grep '^Content-Type: application/vnd.kafka.binary.v2+json$'

# Test GET /topics (get_topics_names)
curl -s 'http://127.0.0.1:8082/topics' | jq -e '(. | sort) == (["'"$NAMESPACE-0"'","'"$NAMESPACE-1"'"] | sort)'

# Test POST /topics/<topic> (post_topics_name)
curl -s "http://127.0.0.1:8082/topics/$NAMESPACE-0" \
  -d '{"records":[{"value":"dmVjdG9yaXplZA==","partition":0},{"value":"cGFuZGFwcm94eQ==","partition":1}]}' \
  -o post_topics_name.json
## successful insertion
jq -e '.offsets[] | select(.partition == 0 and .offset == 1)' post_topics_name.json
## invalid partition
jq -e '.offsets[] | select(.partition == 1 and .error_code == 3)' post_topics_name.json

# Test Metrics
curl -s 'http://127.0.0.1:9645/metrics' | grep 'vectorized_pandaproxy_request_latency_count{operation="get_topics_names",shard="0"}'
curl -s 'http://127.0.0.1:9645/metrics' | grep 'vectorized_pandaproxy_request_latency_count{operation="post_topics_name",shard="0"}'
