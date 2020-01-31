rp_test(
  UNIT_TEST
  BINARY_NAME jitter_tests
  SOURCES jitter_tests.cc
  LIBRARIES Seastar::seastar_testing v::raft

  ARGS "-- -c 1"
 )
rp_test(
  UNIT_TEST
  BINARY_NAME voted_for_config_tests
  SOURCES voted_for_config_tests.cc
  LIBRARIES Seastar::seastar_testing v::raft

  ARGS "-- -c 1"
 )
rp_test(
  UNIT_TEST
  BINARY_NAME log_bootstrap_parsing
  SOURCES bootstrap_configuration_test.cc
  LIBRARIES Seastar::seastar_testing v::raft
  ARGS "-- -c 1"
  )
  rp_test(
    UNIT_TEST
    BINARY_NAME consensus_utils_test
    SOURCES consensus_utils_test.cc
    DEFINITIONS BOOST_TEST_DYN_LINK
    LIBRARIES Boost::unit_test_framework v::raft
)
rp_test(
  UNIT_TEST
  BINARY_NAME foreign_entry_sharing
  SOURCES foreign_entry_test.cc
  LIBRARIES Seastar::seastar_testing v::raft
  )
rp_test(
    UNIT_TEST
    BINARY_NAME group_configuration_tests
    SOURCES group_configuration_tests.cc
    DEFINITIONS BOOST_TEST_DYN_LINK
    LIBRARIES Boost::unit_test_framework v::raft
)
rp_test(
  UNIT_TEST
  BINARY_NAME configuration_serialization_test
  SOURCES configuration_serialization_test.cc
  LIBRARIES Seastar::seastar_testing v::raft
)
rp_test(
  UNIT_TEST
  BINARY_NAME type_serialization_tests
  SOURCES type_serialization_tests.cc
  LIBRARIES Seastar::seastar_testing v::raft
)

rp_test(
  UNIT_TEST
  BINARY_NAME term_assigning_reader_test
  SOURCES term_assigning_reader_test.cc
  LIBRARIES Seastar::seastar_testing v::raft
)

# rp_test(
#   UNIT_TEST
#   BINARY_NAME leadership_test
#   SOURCES leadership_test.cc
#   LIBRARIES Seastar::seastar_testing v::raft
# )

# rp_test(
#   UNIT_TEST
#   BINARY_NAME append_entries_test
#   SOURCES append_entries_test.cc
#   LIBRARIES Seastar::seastar_testing v::raft
# )
