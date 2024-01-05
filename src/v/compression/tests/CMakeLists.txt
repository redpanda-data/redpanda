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
rp_test(
  UNIT_TEST
  BINARY_NAME deltafor_tests
  SOURCES
    delta_for_test.cc
    delta_for_characterization_data.cc
  LIBRARIES v::seastar_testing_main v::compression v::rprandom v::version
  LABELS compression
  ARGS "-- -c 1"
)
rp_test(
  BENCHMARK_TEST
  BINARY_NAME delta_for
  SOURCES delta_for_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::compression
  LABELS compression
)