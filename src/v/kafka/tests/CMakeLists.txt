find_package(CppKafka CONFIG REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME error_mapping_test
  SOURCES error_mapping_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework kafka
)

rp_test(
  UNIT_TEST
  BINARY_NAME member_test
  SOURCES member_test.cc
  LIBRARIES Seastar::seastar_testing kafka
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME group_test
  SOURCES group_test.cc
  LIBRARIES Seastar::seastar_testing kafka
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME read_write_roundtrip_test
  SOURCES read_write_roundtrip_test.cc
  LIBRARIES Seastar::seastar_testing kafka

)

rp_test(
  UNIT_TEST
  BINARY_NAME timeouts_conversion_test
  SOURCES timeouts_conversion_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework kafka
)

rp_test(
  UNIT_TEST
  BINARY_NAME types_conversion_tests
  SOURCES types_conversion_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework kafka
)

rp_test(
  UNIT_TEST
  BINARY_NAME topic_utils_test
  SOURCES topic_utils_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework kafka
)

rp_test(
  UNIT_TEST
  BINARY_NAME topic_result_utils_tests
  SOURCES topic_result_utils_tests.cc
  LIBRARIES Seastar::seastar_testing kafka
)

rp_test(
  UNIT_TEST
  BINARY_NAME metadata_test
  SOURCES metadata_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Seastar::seastar_testing application raft kafka config CppKafka::cppkafka
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME request_parser_test
  SOURCES request_parser_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Seastar::seastar_testing application raft kafka config
  # generate request data as a prepare step. the test and prepare command run in
  # the same scratch directory so the generated output file writes to pwd.
  PREPARE_COMMAND "${V_DEPS_INSTALL_DIR}/bin/kafka-python-env ${PROJECT_SOURCE_DIR}/tools/kafka-python-api-serde.py 1000 > requests.bin"
  ARGS "-- -c 1"
)
