subdir('parsing')
subdir('json')

libpandaproxy_common = library('pandaproxy_common',
  'error.cc',
  'logger.cc',
  'probe.cc',
  'server.cc',
  'kafka_client_cache.cc',
  'sharded_client_cache.cc',
  implicit_include_directories: false,
  dependencies: [config, kafka_client, utils, pandaproxy_parsing, pandaproxy_json])

pandaproxy_common = declare_dependency(
  dependencies: [config, kafka_client, utils, pandaproxy_parsing, pandaproxy_json],
  link_with: libpandaproxy_common)

subdir('api/api-doc')
subdir('schema_registry')
subdir('rest')

t = executable('test_pandaproxy_unit',
  'test/errors.cc',
  dependencies: [pandaproxy_common, pandaproxy_parsing, pandaproxy_json, kafka_protocol])
test('pandaproxy unit', t)
