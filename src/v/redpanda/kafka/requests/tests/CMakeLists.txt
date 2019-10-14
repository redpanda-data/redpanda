find_package(Seastar REQUIRED)
find_package(Boost REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME read_write_roundtrip_test
  SOURCES read_write_roundtrip_test.cc
  LIBRARIES Seastar::seastar_testing requests
  DEFINITIONS SEASTAR_TESTING_MAIN
)

rp_test(
  UNIT_TEST
  BINARY_NAME timeouts_conversion_test
  SOURCES timeouts_conversion_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework requests
)

rp_test(
  UNIT_TEST
  BINARY_NAME types_conversion_tests
  SOURCES types_conversion_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework requests
)