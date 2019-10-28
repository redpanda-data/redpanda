find_package(Boost REQUIRED)
find_package(Seastar REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME record_batch_reader_test
  SOURCES record_batch_reader_test.cc
  LIBRARIES Seastar::seastar_testing model
  DEFINITIONS SEASTAR_TESTING_MAIN
  ARGS "-- -c 1"
)
rp_test(
  UNIT_TEST
  BINARY_NAME ntp_tests
  SOURCES ntp_path_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework model
)
rp_test(
  UNIT_TEST
  BINARY_NAME compression_lexical_cast_test
  SOURCES compression_lexical_cast_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework model
)

rp_test(
  UNIT_TEST
  BINARY_NAME record_batch_attributes_test
  SOURCES record_batch_attributes_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework model
)
