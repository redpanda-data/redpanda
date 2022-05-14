libutils = library('utils',
  'hdr_hist.cc',
  'human.cc',
  'file_io.cc',
  'base64.cc',
  'retry_chain_node.cc',
  'tracking_allocator.cc',
  'vint.cc',
  'directory_walker.cc',
  'uuid.cc',
  'request_auth.cc',
  'bottomless_token_bucket.cc',
  dependencies: [hdr_histogram, base64, bytes, model, json])

utils = declare_dependency(
  dependencies: [hdr_histogram, base64, bytes, model, json],
  link_with: libutils)

exe = executable('test_utils',
  'tests/vint_test.cc',
  'tests/directory_walker_test.cc',
  'tests/outcome_utils_test.cc',
  'tests/base64_test.cc',
  'tests/timed_mutex_test.cc',
  'tests/expiring_promise_test.cc',
  'tests/retry_chain_node_test.cc',
  'tests/input_stream_fanout_test.cc',
  'tests/waiter_queue_test.cc',
  'tests/object_pool_test.cc',
  'tests/delta_for_test.cc',
  'tests/token_bucket_test.cc',
  'tests/uuid_test.cc',
  'tests/seastar_histogram_test.cc',
  dependencies: [utils, seastar_testing_main, random])
test('utils', exe, timeout: 60)

exe = executable('test_utils_threaded',
  'tests/remote_test.cc',
  'tests/retry_test.cc',
  dependencies: [utils, seastar_testing_main])
test('utils', exe)

exe = executable('test_utils_unit',
  'tests/string_switch_test.cc',
  'tests/constexpr_string_switch.cc',
  'tests/named_type_tests.cc',
  'tests/tristate_test.cc',
  'tests/moving_average_test.cc',
  'tests/human_test.cc',
  'tests/fragmented_vector_test.cc',
  'tests/tracking_allocator_tests.cc',
  'tests/bottomless_token_bucket_test.cc',
  'tests/filtered_lower_bound_test.cc',
  dependencies: [utils, boost, absl])
test('utils', exe)
