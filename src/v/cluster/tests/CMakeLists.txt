rp_test(
  UNIT_TEST
  BINARY_NAME partition_allocator
  SOURCES partition_allocator_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework cluster
)
