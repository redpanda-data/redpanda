rp_test(
  UNIT_TEST
  BINARY_NAME partition_allocator
  SOURCES partition_allocator_tests.cc
  LIBRARIES Seastar::seastar_testing cluster
  DEFINITIONS SEASTAR_TESTING_MAIN
)
rp_test(
  BENCHMARK_TEST
  BINARY_NAME partition_allocator_b
  SOURCES allocation_bench.cc
  LIBRARIES cluster Seastar::seastar_perf_testing
)
