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
rp_test(
  UNIT_TEST
  BINARY_NAME log_bootstrap_parsing
  SOURCES bootstrap_configuration_test.cc
  LIBRARIES Seastar::seastar_testing raft
  ARGS "-- -c 1"
  )
  rp_test(
    UNIT_TEST
    BINARY_NAME consensus_utils_test
    SOURCES consensus_utils_test.cc
    DEFINITIONS BOOST_TEST_DYN_LINK
    LIBRARIES Boost::unit_test_framework raft
)
rp_test(
  UNIT_TEST
  BINARY_NAME foreign_entry_sharing
  SOURCES foreign_entry_test.cc
  LIBRARIES Seastar::seastar_testing raft
  )
rp_test(
    UNIT_TEST
    BINARY_NAME group_configuration_tests
    SOURCES group_configuration_tests.cc
    DEFINITIONS BOOST_TEST_DYN_LINK
    LIBRARIES Boost::unit_test_framework raft
)
rp_test(
  UNIT_TEST
  BINARY_NAME configuration_serialization_test
  SOURCES configuration_serialization_test.cc
  LIBRARIES Seastar::seastar_testing raft
)