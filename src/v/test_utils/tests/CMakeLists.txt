rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME gtest_test
  SOURCES gtest_test.cc
  LIBRARIES v::gtest_main)

rp_test(
  UNIT_TEST
  BINARY_NAME test_utils_iostream_test
  SOURCES
    iostream_test.cc
  LIBRARIES v::seastar_testing_main
  ARGS "-- -c 1"
)
