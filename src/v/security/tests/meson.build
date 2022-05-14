t = executable('test_security',
  'scram_algorithm_test.cc',
  'credential_store_test.cc',
  'authorizer_test.cc',
  'mtls_test.cc',
  dependencies: [security, boost, config, random])
test('security', t)
