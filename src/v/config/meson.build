libconfig = library('config',
  'configuration.cc',
  'node_config.cc',
  'base_property.cc',
  'rjson_serialization.cc',
  'validators.cc',
  'broker_authn_endpoint.cc',
  'rest_authn_endpoint.cc',
  dependencies: [seastar, json, absl, model, security, yaml, boost])

config = declare_dependency(
  dependencies: [seastar, json, absl, model, security, yaml, boost],
  link_with: libconfig)

t = executable('test_configuration',
  'tests/bounded_property_test.cc',
  'tests/enum_property_test.cc',
  'tests/retention_property_test.cc',
  'tests/config_store_test.cc',
  'tests/socket_address_convert_test.cc',
  'tests/tls_config_convert_test.cc',
  'tests/advertised_kafka_api_test.cc',
  'tests/seed_server_property_test.cc',
  dependencies: [config, seastar_testing_main])
test('config testing', t)
