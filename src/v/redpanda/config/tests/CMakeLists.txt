find_package(Boost REQUIRED)
find_package(Seastar REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME config_store_test
  SOURCES config_store_test.cc
  LIBRARIES Seastar::seastar_testing config
  DEFINITIONS SEASTAR_TESTING_MAIN
)

rp_test(
  UNIT_TEST
  BINARY_NAME socket_address_yaml_convert_test
  SOURCES socket_address_convert_test.cc
  LIBRARIES Seastar::seastar_testing config
  DEFINITIONS SEASTAR_TESTING_MAIN
)

rp_test(
  UNIT_TEST
  BINARY_NAME tls_config_convert_test
  SOURCES tls_config_convert_test.cc
  LIBRARIES Seastar::seastar_testing config $<TARGET_NAME_IF_EXISTS:Libcxx::libcxx>
  DEFINITIONS SEASTAR_TESTING_MAIN
)