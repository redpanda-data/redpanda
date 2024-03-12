rp_test(
  UNIT_TEST
  BINARY_NAME test_kafka_server
  SOURCES
    error_mapping_test.cc
    timeouts_conversion_test.cc
    types_conversion_tests.cc
    topic_utils_test.cc
    handler_interface_test.cc
    quota_managers_test.cc
    validator_tests.cc
    fetch_unit_test.cc
    config_utils_test.cc
    config_response_utils_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::kafka
  LABELS kafka
)


v_cc_library(
  NAME
    kafka_test_utils
  HDRS
    "delete_records_utils.h"
    "list_offsets_utils.h"
    "offset_for_leader_epoch_utils.h"
    "produce_consume_utils.h"
  SRCS
    "delete_records_utils.cc"
    "list_offsets_utils.cc"
    "offset_for_leader_epoch_utils.cc"
    "produce_consume_utils.cc"
  DEPS
    v::bytes
    v::kafka
    v::storage
)

set(srcs
  consumer_groups_test.cc
  token_bucket_algorithm_test.cc
  member_test.cc
  group_test.cc
  read_write_roundtrip_test.cc
  fetch_test.cc
  delete_topics_test.cc
  offset_fetch_test.cc
  api_versions_test.cc
  usage_manager_test.cc
  create_topics_test.cc
  find_coordinator_test.cc
  list_offsets_test.cc
  topic_recreate_test.cc
  fetch_session_test.cc
  alter_config_test.cc
  produce_consume_test.cc
  group_metadata_serialization_test.cc
  partition_reassignments_test.cc)

rp_test(
  FIXTURE_TEST
  BINARY_NAME kafka_server
  SOURCES ${srcs}
  LIBRARIES v::seastar_testing_main v::application v::raft v::kafka v::config v::storage_test_utils v::kafka_test_utils
  ARGS "-- -c 1"
  LABELS kafka
)


rp_test(
  BENCHMARK_TEST
  BINARY_NAME kafka_fetch_plan
  SOURCES fetch_plan_bench.cc
  LIBRARIES Seastar::seastar_perf_testing Boost::unit_test_framework v::application
  # the args below are just to keep it fast
  ARGS "-c 1 --duration=1 --runs=1 --memory=4G"
  LABELS kafka
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME kafka_produce_partition
  SOURCES produce_partition_bench.cc
  LIBRARIES Seastar::seastar_perf_testing Boost::unit_test_framework v::application
  # the args below are just to keep it fast
  ARGS "-c 1 --duration=1 --runs=1 --memory=4G"
  LABELS kafka
)
