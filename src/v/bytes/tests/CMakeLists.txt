find_package(Boost REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME iobuf_test
  SOURCES iobuf_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework rprandom bytes
  )
