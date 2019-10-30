find_package(Seastar REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME partition_allocator
  SOURCES partition_allocator_tests.cc
  LIBRARIES Seastar::seastar_testing cluster

)
rp_test(
  BENCHMARK_TEST
  BINARY_NAME partition_allocator_b
  SOURCES allocation_bench.cc
  LIBRARIES cluster Seastar::seastar_perf_testing
)
rp_test(
  UNIT_TEST
  BINARY_NAME simple_batch_builder_test
  SOURCES simple_batch_builder_test.cc
  LIBRARIES Seastar::seastar_testing cluster

)
