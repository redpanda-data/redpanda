rp_test(
  UNIT_TEST
  BINARY_NAME serde
  SOURCES serde_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK SERDE_TEST
  LIBRARIES v::seastar_testing_main v::serde
  LABELS serde
)
rp_test(
  BENCHMARK_TEST
  BINARY_NAME serde
  SOURCES bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::serde
  LABELS serde
)
