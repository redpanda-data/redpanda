exe = executable('test_self_test_request',
  'self_test_request_test.cc',
  dependencies: [seastar_testing_main, cluster])
test('cluster', exe, args: '-- -c1')

exe = executable('test_self_test_config',
  'self_test_config_test.cc',
  dependencies: [boost, cluster])
test('cluster', exe, args: '-- -c1')
