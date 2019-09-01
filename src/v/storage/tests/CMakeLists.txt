find_package(Boost REQUIRED)
find_package(Seastar REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME log_segment_appender_test
  SOURCES log_segment_appender_test.cc
  LIBRARIES Seastar::seastar_testing storage
  DEFINITIONS SEASTAR_TESTING_MAIN
  ARGS "-- -c 1"
)