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

# ARGS "-- -c 1"
t = executable('test_http_client',
  'tests/http_client_test.cc',
  'tests/framing_test.cc',
  dependencies: [http, seastar_testing_main])
test('http client', t)

executable('demo_http_client',
  'demo/client.cc',
  dependencies: [http])
