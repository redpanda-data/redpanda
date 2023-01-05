rp_test(
  UNIT_TEST
  BINARY_NAME basic_fast_random_test
  SOURCES random_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main v::bytes v::rprandom Boost::unit_test_framework absl::flat_hash_set
  LABELS random
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME random_bench
  SOURCES random_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::bytes v::rprandom
  LABELS random
)
