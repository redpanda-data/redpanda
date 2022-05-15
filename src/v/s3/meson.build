# -DBOOST_ASIO_HAS_STD_INVOKE_RESULT
libs3 = library('s3',
  'client.cc',
  'error.cc',
  'client_probe.cc',
  implicit_include_directories: false,
  dependencies: [seastar, bytes, http, cloud_roles])

s3 = declare_dependency(
  dependencies: [seastar, bytes, http, cloud_roles],
  link_with: libs3)

# ARGS "-- -c 1"
t = executable('test_s3',
  'tests/s3_client_test.cc',
  dependencies: [s3, seastar_testing_main])
test('http client', t)

executable('demo_s3_test_client',
  'test_client/s3_test_client_main.cc',
  dependencies: [s3])
