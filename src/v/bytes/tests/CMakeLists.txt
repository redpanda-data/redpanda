find_package(Boost REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME bytes_ostream_test
  SOURCES bytes_ostream_test.cc
  INCLUDES ${PROJECT_SOURCE_DIR}/src/v/bytes
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework rprandom bytes
)
