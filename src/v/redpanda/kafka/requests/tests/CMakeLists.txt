find_package(Seastar REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME read_write_roundtrip_test
  SOURCES read_write_roundtrip_test.cc
  LIBRARIES Seastar::seastar_testing requests
  DEFINITIONS SEASTAR_TESTING_MAIN
 )