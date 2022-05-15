cycling_service_h = custom_target(
  output: 'cycling_service.h',
  input: 'cycling_service.json',
  command: [rpc_compiler, '--service_file', '@INPUT@',
    '--output_file', '@OUTPUT@'])

echo_service_h = custom_target(
  output: 'echo_service.h',
  input: 'echo_service.json',
  command: [rpc_compiler, '--service_file', '@INPUT@',
    '--output_file', '@OUTPUT@'])

echo_v2_service_h = custom_target(
  output: 'echo_v2_service.h',
  input: 'echo_v2_service.json',
  command: [rpc_compiler, '--service_file', '@INPUT@',
    '--output_file', '@OUTPUT@'])

# ARGS "-- -c 1"
t = executable('test_rpc',
  'netbuf_tests.cc',
  'roundtrip_tests.cc',
  'response_handler_tests.cc',
  'serialization_test.cc',
  dependencies: [rpc, seastar_testing_main])
test('rpc', t)

#
# TODO looks like this needs to have the tls inputs setup
#
# INPUT_FILES ${CMAKE_CURRENT_SOURCE_DIR}/redpanda.crt
#             ${CMAKE_CURRENT_SOURCE_DIR}/redpanda.key
#             ${CMAKE_CURRENT_SOURCE_DIR}/root_certificate_authority.chain_cert
#             ${CMAKE_CURRENT_SOURCE_DIR}/redpanda.other.crt
#             ${CMAKE_CURRENT_SOURCE_DIR}/redpanda.other.key
#             ${CMAKE_CURRENT_SOURCE_DIR}/root_certificate_authority.other.chain_cert
# ARGS "-- -c 1"
t = executable('test_rpc_generator',
  'rpc_gen_cycling_test.cc',
  cycling_service_h,
  echo_service_h,
  echo_v2_service_h,
  dependencies: [rpc, common, seastar_testing_main])
test('rpc generator', t)

t = executable('test_exponential_backoff',
  'exponential_backoff.cc',
  dependencies: [boost, rpc])
test('exponential backoff', t)

t = executable('bench_rpc_serialization',
  'rpc_bench.cc',
  dependencies: [rpc, seastar_perf_testing])
benchmark('rpc serialization', t)
