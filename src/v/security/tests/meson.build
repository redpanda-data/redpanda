exe = executable('test_security',
  'scram_algorithm_test.cc',
  'credential_store_test.cc',
  'authorizer_test.cc',
  'mtls_test.cc',
  'license_test.cc',
  'gssapi_principal_mapper_test.cc',
  dependencies: [security, boost, config, random])
test('security', exe)

exe = executable('test_security_thread',
  'ephemeral_credential_store_test.cc',
  dependencies: [seastar_testing_main, security])
test('security', exe)
