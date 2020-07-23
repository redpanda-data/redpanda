#!/bin/bash
set -e
set -x

BAMBOO_BROKERS=${BAMBOO_BROKERS:-localhost:9092}

cd /opt/librdkafka/tests
echo "metadata.broker.list=${BAMBOO_BROKERS}" >test.conf

skipped_tests=(
  19 # segfault in group membership https://app.clubhouse.io/vectorized/story/963/dereferencing-null-pointer-in-kafka-group-membership
  30 # fix in v-dev: https://app.clubhouse.io/vectorized/story/928/offset-commit-reporting-success-for-non-existent-topic
  52 # https://app.clubhouse.io/vectorized/story/997/librdkafka-tests-failing-due-to-consumer-out-of-range-timestamps
  54 # timequery issues: https://app.clubhouse.io/vectorized/story/995/librdkafka-offset-time-query
  63 # cluster-id: https://app.clubhouse.io/vectorized/story/939/generate-cluster-id-uuid-on-bootstrap-and-expose-through-metadata-request
  67 # empty topic offset edge case: https://app.clubhouse.io/vectorized/story/940/consuming-from-empty-topic-should-return-eof
  77 # topic compaction settings: https://app.clubhouse.io/vectorized/story/999/support-create-topic-configurations-for-compaction-retention-policies
  82 # looks like corrupt fetch response: https://app.clubhouse.io/vectorized/story/998/librdkafka-test-appears-to-cause-fetch-to-return-corrupt-message-data
  92 # no support for v2 -> v1 message version conversion in the broker
  44 # we do not support runtime changes to topic partition count
  69 # we do not support runtime changes to topic partition count
  81 # we do not support replica assignment
  61 # transactions
  76 # idempotent producer
  86 # idempotent producer
  90 # idempotent producer
  94 # idempotent producer
  98 # transactions
)

function skip_test() {
  for skipped in "${skipped_tests[@]}"; do
    if [[ ${skipped} == ${1} ]]; then
      return 0
    fi
  done
  return 1
}

for num in $(seq 0 100); do
  ! skip_test ${num} || continue
  TESTS=$(printf "%04d" ${num}) make
done
