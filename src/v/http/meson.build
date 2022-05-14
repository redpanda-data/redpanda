# -DBOOST_ASIO_HAS_STD_INVOKE_RESULT
libhttp = library('http',
  'iobuf_body.cc',
  'chunk_encoding.cc',
  'client.cc',
  'logger.cc',
  implicit_include_directories: false,
  dependencies: [boost, bytes, net])

http = declare_dependency(
  dependencies: [boost, bytes, net],
  link_with: libhttp)

executable('demo_http_client',
  'demo/client.cc',
  dependencies: [http])

libhttp_test_utils = library('http_test_utils',
  'tests/http_imposter.cc',
  'tests/registered_urls.cc',
  dependencies: [net, serde, utils],
  implicit_include_directories: false)

http_test_utils = declare_dependency(
  dependencies: [net, serde, utils],
  link_with: libhttp_test_utils)

exe = executable('test_http_client',
  'tests/http_client_test.cc',
  'tests/framing_test.cc',
  dependencies: [seastar_testing_main, http])
test('http', exe, args: ['-- -c1'])

exe = executable('test_http_imposter',
  'tests/http_imposter_test.cc',
  dependencies: [seastar_testing_main, http_test_utils, http])
test('http', exe, args: ['-- -c1'])
