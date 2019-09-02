find_package(Boost REQUIRED)
find_package(Seastar REQUIRED)

rp_test(
  UNIT_TEST
  BINARY_NAME record_batch_reader_test
  SOURCES record_batch_reader_test.cc
  LIBRARIES Seastar::seastar_testing model
  DEFINITIONS SEASTAR_TESTING_MAIN
  ARGS "-- -c 1"
)
