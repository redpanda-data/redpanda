libcloud_roles = library('cloud_roles',
  'apply_aws_credentials.cc',
  'apply_credentials.cc',
  'apply_gcp_credentials.cc',
  'aws_refresh_impl.cc',
  'aws_sts_refresh_impl.cc',
  'gcp_refresh_impl.cc',
  'probe.cc',
  'refresh_credentials.cc',
  'request_response_helpers.cc',
  'types.cc',
  'signature.cc',
  implicit_include_directories: false,
  dependencies: [seastar, http, model, utils])

cloud_roles = declare_dependency(
  dependencies: [seastar, http, model, utils],
  link_with: libcloud_roles)

# ARGS "-- -c 1"
t = executable('test_signature',
  'tests/signature_test.cc',
  dependencies: [seastar_testing_main, cloud_roles])
test('cloud roles signature', t)

executable('test_cloud_roles',
  'tests/role_client_tests.cc',
  'tests/categorization_tests.cc',
  'tests/fetch_credentials_tests.cc',
  'tests/credential_tests.cc',
  dependencies: [seastar_testing_main, cloud_roles])
