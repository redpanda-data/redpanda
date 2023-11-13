set(srcs
    bounded_property_test.cc
    enum_property_test.cc
    retention_property_test.cc
    config_store_test.cc
    socket_address_convert_test.cc
    tls_config_convert_test.cc
    scoped_config_test.cc
    advertised_kafka_api_test.cc
    seed_server_property_test.cc
    cloud_credentials_source_test.cc
    validator_tests.cc
    throughput_control_group_test.cc)

rp_test(
  UNIT_TEST
  BINARY_NAME test_configuration
  SOURCES ${srcs}
  LIBRARIES v::seastar_testing_main v::config
  LABELS config
)

set(gtest_srcs
    config_constraints_gtests.cc
    validator_gtests.cc)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME gtest_configuration
  SOURCES ${gtest_srcs}
  LIBRARIES v::gtest_main v::config v::kafka)
