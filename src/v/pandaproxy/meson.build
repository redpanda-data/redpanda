subdir('parsing')
subdir('json')

libpandaproxy_common = library('pandaproxy_common',
  'error.cc',
  'logger.cc',
  'probe.cc',
  'server.cc',
  'kafka_client_cache.cc',
  'config_utils.cc',
  implicit_include_directories: false,
  dependencies: [config, kafka_client, utils, pandaproxy_parsing, pandaproxy_json])

pandaproxy_common = declare_dependency(
  dependencies: [config, kafka_client, utils, pandaproxy_parsing, pandaproxy_json],
  link_with: libpandaproxy_common)

subdir('api/api-doc')
subdir('schema_registry')
subdir('rest')
