libconfig = library('config',
  'rest_authn_endpoint.cc',
  'broker_authn_endpoint.cc',
  'client_group_byte_rate_quota.cc',
  'configuration.cc',
  'node_config.cc',
  'base_property.cc',
  'rjson_serialization.cc',
  'validators.cc',
  dependencies: [seastar, json, absl, model, security, yaml, boost])

config = declare_dependency(
  dependencies: [seastar, json, absl, model, security, yaml, boost],
  link_with: libconfig)

exe = executable('test_configuration',
  'tests/bounded_property_test.cc',
  'tests/enum_property_test.cc',
  'tests/retention_property_test.cc',
  'tests/config_store_test.cc',
  'tests/socket_address_convert_test.cc',
  'tests/tls_config_convert_test.cc',
  'tests/advertised_kafka_api_test.cc',
  'tests/seed_server_property_test.cc',
  'tests/cloud_credentials_source_test.cc',
  'tests/validator_tests.cc',
  dependencies: [seastar_testing_main, config])
test('config', exe)
