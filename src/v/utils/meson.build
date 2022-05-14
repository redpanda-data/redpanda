libutils = library('utils',
  'hdr_hist.cc',
  'human.cc',
  'file_io.cc',
  'base64.cc',
  'retry_chain_node.cc',
  'vint.cc',
  dependencies: [hdr_histogram, base64, bytes, model])

utils = declare_dependency(
  dependencies: [hdr_histogram, base64, bytes, model],
  link_with: libutils)

# ARGS "-- -c 1"
t = executable('test_utils',
  'tests/vint_test.cc',
  'tests/directory_walker_test.cc',
  'tests/outcome_utils_test.cc',
  'tests/base64_test.cc',
  'tests/timed_mutex_test.cc',
  'tests/expiring_promise_test.cc',
  'tests/retry_chain_node_test.cc',
  'tests/input_stream_fanout_test.cc',
  'tests/waiter_queue_test.cc',
  'tests/delta_for_test.cc',
  dependencies: [utils, seastar_testing_main, random])
test('utils', t)

# ARGS "-- -c 2"
t = executable('test_utils_threaded',
  'tests/remote_test.cc',
  'tests/retry_test.cc',
  dependencies: [utils, seastar_testing_main])
test('utils threaded', t)

t = executable('test_utils_unit',
  'tests/string_switch_test.cc',
  'tests/constexpr_string_switch.cc',
  'tests/named_type_tests.cc',
  'tests/tristate_test.cc',
  'tests/moving_average_test.cc',
  'tests/human_test.cc',
  'tests/fragmented_vector_test.cc',
  dependencies: [utils, boost])
test('utils unit', t)
