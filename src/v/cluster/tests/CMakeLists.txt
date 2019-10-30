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
rp_test(
  UNIT_TEST
  BINARY_NAME serialization_rt_test
  SOURCES serialization_rt_test.cc
  LIBRARIES Seastar::seastar_testing cluster
  DEFINITIONS SEASTAR_TESTING_MAIN
)
rp_test(
  UNIT_TEST
  BINARY_NAME controller_recovery_test
  SOURCES controller_recovery_test.cc
  LIBRARIES Seastar::seastar_testing cluster
)
rp_test(
  UNIT_TEST
  BINARY_NAME cache_types_tests
  SOURCES cache_types_tests.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework cluster
)
rp_test(
  UNIT_TEST
  BINARY_NAME metadata_cache_tests
  SOURCES metadata_cache_tests.cc
  LIBRARIES Seastar::seastar_testing cluster
  DEFINITIONS SEASTAR_TESTING_MAIN
)
