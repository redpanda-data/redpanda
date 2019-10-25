rp_test(
  UNIT_TEST
  BINARY_NAME member_test
  SOURCES member_test.cc
  LIBRARIES Seastar::seastar_testing groups
  DEFINITIONS SEASTAR_TESTING_MAIN
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  BINARY_NAME group_test
  SOURCES group_test.cc
  LIBRARIES Seastar::seastar_testing groups
  DEFINITIONS SEASTAR_TESTING_MAIN
  ARGS "-- -c 1"
)
