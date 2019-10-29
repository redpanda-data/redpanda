find_package(Seastar REQUIRED)
rp_test(
  UNIT_TEST
  BINARY_NAME jitter_tests
  SOURCES jitter_tests.cc
  LIBRARIES Seastar::seastar_testing raft

  ARGS "-- -c 1"
 )
rp_test(
  UNIT_TEST
  BINARY_NAME voted_for_config_tests
  SOURCES voted_for_config_tests.cc
  LIBRARIES Seastar::seastar_testing raft

  ARGS "-- -c 1"
 )
rp_test(
  UNIT_TEST
  BINARY_NAME exponential_backoff
  SOURCES exponential_backoff.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES Boost::unit_test_framework raft
)
