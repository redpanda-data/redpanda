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
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::kafka v::coproc
  LABELS kafka
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
  LIBRARIES v::seastar_testing_main v::application v::raft v::kafka v::config v::storage_test_utils
  ARGS "-- -c 1"
  LABELS kafka
)
