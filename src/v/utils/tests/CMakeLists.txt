SET(HANDLE_SIGTERM_SCRIPT "${CMAKE_CURRENT_SOURCE_DIR}/handle-sigterm.sh")

rp_test(
  UNIT_TEST
  BINARY_NAME utils_single_thread
  SOURCES
    base64_test.cc
    delta_for_test.cc
    delta_for_characterization_data.cc
    directory_walker_test.cc
    expiring_promise_test.cc
    input_stream_fanout_test.cc
    object_pool_test.cc
    retry_chain_node_test.cc
    seastar_histogram_test.cc
    timed_mutex_test.cc
    rwlock_test.cc
    token_bucket_test.cc
    uuid_test.cc
    vint_test.cc
    waiter_queue_test.cc
    auto_fmt_test.cc
  LIBRARIES v::seastar_testing_main v::utils v::bytes v::version absl::flat_hash_set
  ARGS "-- -c 1"
  LABELS utils
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME gtest_utils_single_thread
  SOURCES
    external_process_test.cc
  LIBRARIES
    v::utils
    v::gtest_main
  LABELS utils
  ARGS "-- -c 1"
  ENV
    "HANDLE_SIGTERM_SCRIPT=${HANDLE_SIGTERM_SCRIPT}"
)

rp_test(
  UNIT_TEST
  BINARY_NAME utils_multi_thread
  SOURCES
    remote_test.cc
    retry_test.cc
  LIBRARIES v::seastar_testing_main
  ARGS "-- -c 2"
  LABELS utils
)

rp_test(
  UNIT_TEST
  BINARY_NAME utils_unit
  SOURCES
    bottomless_token_bucket_test.cc
    filtered_lower_bound_test.cc
    human_test.cc
    move_canary_test.cc
    moving_average_test.cc
    named_type_tests.cc
    stable_iterator_test.cc
    tracking_allocator_tests.cc
    tristate_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::utils absl::flat_hash_map v::random
  LABELS utils
)


rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME gtest_utils
  SOURCES
    xid_test.cc
  LIBRARIES v::utils absl::flat_hash_map v::random v::gtest_main
  LABELS utils
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME seastar_histogram
  SOURCES seastar_histogram_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::utils
  LABELS utils
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME delta_for
  SOURCES delta_for_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::utils
  LABELS utils
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME coro
  SOURCES
    coro_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::utils
  LABELS utils
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME vint
  SOURCES
    vint_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::utils
  LABELS utils
)
