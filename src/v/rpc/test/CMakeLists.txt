find_package(Boost REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME rpc_serialization_tests
  SOURCES serialization_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework rpc
)
rp_test(
  UNIT_TEST
  BINARY_NAME roundtrip_deserialize
  SOURCES roundtrip_tests.cc
  LIBRARIES Seastar::seastar_testing rpc
  DEFINITIONS SEASTAR_TESTING_MAIN
  ARGS "-- -c 1"
)
