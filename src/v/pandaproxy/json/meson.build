libpandaproxy_json = library('pandaproxy_json',
  'error.cc',
  implicit_include_directories: false,
  dependencies: [json, model, kafka_protocol])

pandaproxy_json = declare_dependency(
  dependencies: [json, model, kafka_protocol],
  link_with: libpandaproxy_json)

exe = executable('test_pp_json_types',
  'test/iobuf.cc',
  dependencies: [seastar_testing_main, pandaproxy_json, utils])
test('panda proxy json types', exe)

exe = executable('test_pp_json_requests',
  'requests/test/fetch.cc',
  'requests/test/produce.cc',
  dependencies: [seastar_testing_main, pandaproxy_json, utils, kafka_server, kafka_client])
test('panda proxy json requests', exe)
