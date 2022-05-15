libcloud_storage_clients = library('cloud_storage_clients',
  'abs_client.cc',
  'abs_error.cc',
  'client_pool.cc',
  'client_probe.cc',
  'configuration.cc',
  's3_client.cc',
  's3_error.cc',
  'util.cc',
  'xml_sax_parser.cc',
  implicit_include_directories: false,
  dependencies: [seastar, bytes, http, cloud_roles, xml2])

cloud_storage_clients = declare_dependency(
  dependencies: [seastar, bytes, http, cloud_roles, xml2],
  link_with: libcloud_storage_clients)

exe = executable('test_s3',
  'tests/backend_detection_test.cc',
  'tests/s3_client_test.cc',
  'tests/xml_sax_parser_test.cc',
  dependencies: [seastar_testing_main, cloud_storage_clients])
test('cloud storage clients', exe)

executable('demo_s3_test_client',
  'test_client/s3_test_client_main.cc',
  dependencies: [cloud_storage_clients])

executable('demo_abs_test_client',
  'test_client/abs_test_client_main.cc',
  dependencies: [cloud_storage_clients])
