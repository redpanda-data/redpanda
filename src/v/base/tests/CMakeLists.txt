rp_test(
  UNIT_TEST
  BINARY_NAME outcome_test
  SOURCES outcome_test.cc
  LIBRARIES Boost::unit_test_framework v::base)

rp_test(
  UNIT_TEST
  BINARY_NAME outcome_utils_test
  SOURCES outcome_utils_test.cc
  LIBRARIES v::seastar_testing_main v::base)
