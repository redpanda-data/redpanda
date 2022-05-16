subdir('admin/api-doc')

libredpanda_app = library('redpanda_app',
  'admin_server.cc',
  'cli_parser.cc',
  'cluster_config_schema_util.cc',
  'application.cc',
  admin_api_h,
  implicit_include_directories: false,
  dependencies: [cluster, config, kafka_server, coproc,
    pandaproxy_schema_registry, pandaproxy_rest, archival])

redpanda_app = declare_dependency(
  dependencies: [cluster, config, kafka_server, coproc,
    pandaproxy_schema_registry, pandaproxy_rest, archival],
  link_with: libredpanda_app)

executable('redpanda',
  'main.cc',
  dependencies: [redpanda_app])

t = executable('test_cli_parser',
  'tests/cli_parser_tests.cc',
  dependencies: [boost, redpanda_app])
test('cli parser', t)
