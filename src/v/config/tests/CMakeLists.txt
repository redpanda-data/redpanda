
rp_test(
  UNIT_TEST
  BINARY_NAME config_store_test
  SOURCES config_store_test.cc
  LIBRARIES Seastar::seastar_testing v::config

)

rp_test(
  UNIT_TEST
  BINARY_NAME socket_address_yaml_convert_test
  SOURCES socket_address_convert_test.cc
  LIBRARIES Seastar::seastar_testing v::config

)

rp_test(
  UNIT_TEST
  BINARY_NAME tls_config_convert_test
  SOURCES tls_config_convert_test.cc
  LIBRARIES Seastar::seastar_testing v::config

)

rp_test(
  UNIT_TEST
  BINARY_NAME advertised_kafka_api_test
  SOURCES advertised_kafka_api_test.cc
  LIBRARIES Seastar::seastar_testing v::config

)
