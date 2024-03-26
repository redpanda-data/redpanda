rp_test(
  UNIT_TEST
  BINARY_NAME container_thread
  SOURCES
    fragmented_vector_async_test.cc
  LIBRARIES v::seastar_testing_main
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME container_unit
  SOURCES
    contiguous_range_map_test.cc
    chunked_hash_map_test.cc
    interval_set_test.cc
    fragmented_vector_test.cc
  LIBRARIES v::gtest_main v::random v::serde
  ARGS "-- -c 1"
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME container
  SOURCES 
    vector_bench.cc
    map_bench.cc
  LIBRARIES 
    Seastar::seastar_perf_testing
    v::seastar_testing_main
)
