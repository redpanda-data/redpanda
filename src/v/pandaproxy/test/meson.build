exe = executable('test_pandaproxy_unit',
  'errors.cc',
  dependencies: [boost, pandaproxy_common, pandaproxy_parsing, pandaproxy_json, kafka_protocol])
test('pandaproxy unit', exe)

exe = executable('test_pandaproxy_thread',
  'one_shot.cc',
  'kafka_client_cache.cc',
  dependencies: [seastar_testing_main, pandaproxy_common, pandaproxy_parsing, pandaproxy_json, kafka_protocol])
test('pandaproxy thread', exe)

exe = executable('test_pandaproxy_rpfixture',
  'test_config_utils.cc',
  dependencies: [seastar_testing_main, pandaproxy_common, pandaproxy_parsing, pandaproxy_json, kafka_protocol, redpanda_app])
test('pandaproxy fixture', exe, is_parallel: false)
