find_package(Boost REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME error_mapping_test
  SOURCES error_mapping_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework errors
)
