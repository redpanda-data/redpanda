set(srcs
    bounded_property_test.cc
    enum_property_test.cc
    retention_property_test.cc
    config_store_test.cc
    socket_address_convert_test.cc
    tls_config_convert_test.cc
    advertised_kafka_api_test.cc
    seed_server_property_test.cc
    cloud_credentials_source_test.cc
    validator_tests.cc)

rp_test(
  UNIT_TEST
  BINARY_NAME test_configuration
  SOURCES ${srcs}
  LIBRARIES v::seastar_testing_main v::config
  LABELS config
)
