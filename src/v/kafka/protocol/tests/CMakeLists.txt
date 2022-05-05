find_package(CppKafka CONFIG REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME
    test_kafka_protocol_unit
  SOURCES
    security_test.cc
  DEFINITIONS
    BOOST_TEST_DYN_LINK
  LIBRARIES
  Boost::unit_test_framework
    v::kafka
    v::storage_test_utils
  LABELS
    kafka
    kafka_protocol
)

rp_test(
  UNIT_TEST
  BINARY_NAME
    test_kafka_protocol_single_thread
  SOURCES
    batch_reader_test.cc
  DEFINITIONS
    BOOST_TEST_DYN_LINK
  LIBRARIES
    v::seastar_testing_main
    v::kafka
    v::storage_test_utils
  ARGS "-- -c 1"
  LABELS
    kafka
    kafka_protocol
)
