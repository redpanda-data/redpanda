rp_test(
  BENCHMARK_TEST
  BINARY_NAME zstd_stream
  SOURCES zstd_stream_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::compression v::rprandom
  LABELS compression
)
rp_test(
  UNIT_TEST
  BINARY_NAME zstd_tests
  SOURCES zstd_tests.cc
  LIBRARIES v::seastar_testing_main v::compression v::rprandom
  LABELS compression
  ARGS "-- -c 1"
  )
