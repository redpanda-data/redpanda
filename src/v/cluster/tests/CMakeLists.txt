rp_test(
  UNIT_TEST
  BINARY_NAME partition_allocator
  SOURCES partition_allocator_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework cluster
)
rp_test(
  BENCHMARK_TEST
  BINARY_NAME partition_allocator_b
  SOURCES allocation_bench.cc
  LIBRARIES cluster Seastar::seastar_perf_testing
)
