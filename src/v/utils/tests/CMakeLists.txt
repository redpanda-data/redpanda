
rp_test(
  UNIT_TEST
  BINARY_NAME utils_single_thread
  SOURCES
    vint_test.cc
    directory_walker_test.cc
    outcome_utils_test.cc
    base64_test.cc
    timed_mutex_test.cc
    expiring_promise_test.cc
    retry_chain_node_test.cc
    input_stream_fanout_test.cc
    waiter_queue_test.cc
    object_pool_test.cc
    delta_for_test.cc
    token_bucket_test.cc
    uuid_test.cc
    seastar_histogram_test.cc
  LIBRARIES v::seastar_testing_main v::utils v::bytes absl::flat_hash_set
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
    string_switch_test.cc
    constexpr_string_switch.cc
    named_type_tests.cc
    tristate_test.cc
    moving_average_test.cc
    human_test.cc
    fragmented_vector_test.cc
    tracking_allocator_tests.cc
    bottomless_token_bucket_test.cc
    filtered_lower_bound_test.cc
    utf8_control_chars.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework v::utils absl::flat_hash_map
  LABELS utils
)
