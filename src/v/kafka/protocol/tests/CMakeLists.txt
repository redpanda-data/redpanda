find_package(CppKafka CONFIG REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME
    test_kafka_protocol
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
