find_package(Boost REQUIRED)
find_package(Seastar REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME config_store_test
  SOURCES config_store_test.cc
  LIBRARIES Seastar::seastar_testing config $<TARGET_NAME_IF_EXISTS:Libcxx::libcxx>
  DEFINITIONS SEASTAR_TESTING_MAIN
)