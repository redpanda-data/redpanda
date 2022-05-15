libpandaproxy_rest = library('pandaproxy_rest',
  'api.cc',
  'configuration.cc',
  'handlers.cc',
  'proxy.cc',
  pandaproxy_api_h,
  implicit_include_directories: false,
  dependencies: [config, kafka_client, pandaproxy_common])

pandaproxy_rest = declare_dependency(
  sources: pandaproxy_api_h,
  dependencies: [config, kafka_client, pandaproxy_common],
  link_with: libpandaproxy_rest)
