find_package(CppKafka CONFIG REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME test_kafka_error_mapping
  SOURCES error_mapping_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::kafka
)

rp_test(
  UNIT_TEST
  BINARY_NAME test_kafka_timeouts_conversion
  SOURCES timeouts_conversion_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::kafka
)

rp_test(
  UNIT_TEST
  BINARY_NAME test_kafka_types_conversion
  SOURCES types_conversion_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::kafka
)

rp_test(
  UNIT_TEST
  BINARY_NAME test_kafka_topic_utils
  SOURCES topic_utils_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::kafka
)

set(srcs
  member_test.cc
  group_test.cc
  read_write_roundtrip_test.cc
  metadata_test.cc
  fetch_test.cc
  delete_topics_test.cc
  offset_fetch_test.cc
  api_versions_test.cc
  create_topics_test.cc
  find_coordinator_test.cc
  list_offsets_test.cc
  offset_commit_test.cc
  topic_recreate_test.cc
  fetch_session_test.cc
  produce_consume_test.cc)

rp_test(
  UNIT_TEST
  BINARY_NAME test_kafka
  SOURCES ${srcs}
  LIBRARIES v::seastar_testing_main v::application v::raft v::kafka v::config v::storage_test_utils
  ARGS "-- -c 1"
)

find_program(KAFKA_PYTHON_ENV "kafka-python-env")

rp_test(
  UNIT_TEST
  BINARY_NAME test_kafka_request_parser
  SOURCES request_parser_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::application v::raft v::kafka
  # generate request data as a prepare step. the test and prepare command run in
  # the same scratch directory so the generated output file writes to pwd.
  PREPARE_COMMAND "${KAFKA_PYTHON_ENV} ${PROJECT_SOURCE_DIR}/tools/kafka-python-api-serde.py 1000 > requests.bin"
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME test_scram_algorithm
  SOURCES scram_algorithm_test.cc
  LIBRARIES v::seastar_testing_main v::kafka
)
