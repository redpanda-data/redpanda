t = executable('test_kafka_server',
  'error_mapping_test.cc',
  'timeouts_conversion_test.cc',
  'types_conversion_tests.cc',
  'topic_utils_test.cc',
  dependencies: [kafka_server, boost])
test('kafka server', t)

# ARGS "-- -c 1"
t = executable('test_kafka_server_fixture',
  'consumer_groups_test.cc',
  'member_test.cc',
  'group_test.cc',
  'read_write_roundtrip_test.cc',
  'fetch_test.cc',
  'delete_topics_test.cc',
  'offset_fetch_test.cc',
  'api_versions_test.cc',
  'create_topics_test.cc',
  'find_coordinator_test.cc',
  'list_offsets_test.cc',
  'topic_recreate_test.cc',
  'fetch_session_test.cc',
  'alter_config_test.cc',
  'produce_consume_test.cc',
  'group_metadata_serialization_test.cc',
  dependencies: [kafka_server, seastar_testing_main, redpanda_app,
    storage_test_utils, model_test_utils])
test('kafka server', t)

#
# 1. propose delete for limited value, librdkakfa dependency
#
# 'metadata_test.cc', librdkafka dep

#
# 2. propose delete for very limited value nowdays
#
#find_program(KAFKA_PYTHON_ENV "kafka-python-env")
#rp_test(
#  UNIT_TEST
#  BINARY_NAME test_kafka_request_parser
#  SOURCES request_parser_test.cc
#  DEFINITIONS BOOST_TEST_DYN_LINK
#  LIBRARIES v::seastar_testing_main v::application v::raft v::kafka
#  # generate request data as a prepare step. the test and prepare command run in
#  # the same scratch directory so the generated output file writes to pwd.
#  PREPARE_COMMAND "${KAFKA_PYTHON_ENV} ${PROJECT_SOURCE_DIR}/tools/kafka-python-api-serde.py 1000 > requests.bin"
#  ARGS "-- -c 1"
#  LABELS kafka
#)
