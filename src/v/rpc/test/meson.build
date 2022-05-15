rpc_test_services = [
  'cycling_service',
  'echo_service',
  'echo_v2_service',
]

rpc_test_services_h = []
foreach service : rpc_test_services
  t = custom_target(
    output: f'@service@.h',
    input: f'@service@.json',
    command: [rpc_compiler, '--service_file', '@INPUT@',
      '--output_file', '@OUTPUT@'])
  rpc_test_services_h += [t]
endforeach

exe = executable('test_rpc',
  'netbuf_tests.cc',
  'roundtrip_tests.cc',
  'response_handler_tests.cc',
  'serialization_test.cc',
  dependencies: [seastar_testing_main, rpc])
test('rpc', exe)

exe = executable('test_rpc_generator',
  'rpc_gen_cycling_test.cc',
  rpc_test_services_h,
  dependencies: [seastar_testing_main, rpc])

cert_files = [
  'redpanda.crt',
  'redpanda.key',
  'root_certificate_authority.chain_cert',
  'redpanda.other.crt',
  'redpanda.other.key',
  'root_certificate_authority.other.chain_cert']

cert_file_args = []
foreach f : cert_files
  cert_file_args += ['--copy_file', join_paths(meson.current_source_dir(), f)]
endforeach

test('rpc', exe,
  timeout: 60,
  args: cert_file_args + ['-- -c1'])

exe = executable('test_exponential_backoff',
  'exponential_backoff.cc',
  dependencies: [boost, rpc])
test('rpc', exe)

exe = executable('bench_rpc_serialization',
  'rpc_bench.cc',
  dependencies: [seastar_perf_testing, rpc])
benchmark('rpc', exe)
