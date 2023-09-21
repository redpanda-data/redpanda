
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
    outcome_utils_test.cc
    retry_chain_node_test.cc
    seastar_histogram_test.cc
    timed_mutex_test.cc
    rwlock_test.cc
    token_bucket_test.cc
    uuid_test.cc
    vint_test.cc
    waiter_queue_test.cc
  LIBRARIES v::seastar_testing_main v::utils v::bytes v::version absl::flat_hash_set
  ARGS "-- -c 1"
  LABELS utils
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
    constexpr_string_switch.cc
    filtered_lower_bound_test.cc
    fragmented_vector_test.cc
    gate_guard_test.cc
    human_test.cc
    move_canary_test.cc
    moving_average_test.cc
    named_type_tests.cc
    stable_iterator_test.cc
    string_switch_test.cc
    tracking_allocator_tests.cc
    tristate_test.cc
    utf8_control_chars.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::utils absl::flat_hash_map
  LABELS utils
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME seastar_histogram
  SOURCES seastar_histogram_bench.cc
  LIBRARIES Seastar::seastar_perf_testing v::utils
  LABELS utils
)
