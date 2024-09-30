set(KAFKA_REQUEST_GENERATOR "${CMAKE_BINARY_DIR}/src/v/test_utils/go/kreq-gen/kafka-request-generator")

rp_test(
  UNIT_TEST
  BINARY_NAME
    test_kafka_protocol_unit
  SOURCES
    security_test.cc
    protocol_test.cc
  DEFINITIONS
    BOOST_TEST_DYN_LINK
  LIBRARIES
  Boost::unit_test_framework
    v::kafka
    v::storage_test_utils
  LABELS
    kafka
    kafka_protocol
  BUILD_DEPENDENCIES
    kafka_request_generator
  ENV
    "GENERATOR_BIN=${KAFKA_REQUEST_GENERATOR}"
)

rp_test(
  UNIT_TEST
  BINARY_NAME
    test_kafka_protocol_single_thread
  SOURCES
    field_parser_test.cc
    batch_reader_test.cc
  DEFINITIONS
    BOOST_TEST_DYN_LINK
  LIBRARIES
    v::seastar_testing_main
    v::kafka
    v::storage_test_utils
    v::application
  ARGS "-- -c 1"
  LABELS
    kafka
    kafka_protocol
)
