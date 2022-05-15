libpandaproxy_schema_registry = library('pandaproxy_schema_registry',
  'api.cc',
  'configuration.cc',
  'handlers.cc',
  'error.cc',
  'service.cc',
  'seq_writer.cc',
  'sharded_store.cc',
  'types.cc',
  'avro.cc',
  'protobuf.cc',
  pandaproxy_api_h,
  implicit_include_directories: false,
  dependencies: [avro, kafka_client, kafka_server, protobuf, pandaproxy_common,
    pandaproxy_json, pandaproxy_parsing])

pandaproxy_schema_registry = declare_dependency(
  sources: pandaproxy_api_h,
  dependencies: [avro, kafka_client, kafka_server, protobuf, pandaproxy_common,
    pandaproxy_json, pandaproxy_parsing],
  link_with: libpandaproxy_schema_registry)

t = executable('test_schema_reg_requests',
  'requests/test/get_subject_versions_version.cc',
  'requests/test/post_subject_versions.cc',
  dependencies: [seastar_testing_main, pandaproxy_schema_registry, utils])
test('schema registry requests', t)
