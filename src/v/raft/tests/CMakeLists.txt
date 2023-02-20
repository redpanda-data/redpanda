rp_test(
    UNIT_TEST
    BINARY_NAME consensus_utils_test
    SOURCES consensus_utils_test.cc
    DEFINITIONS BOOST_TEST_DYN_LINK
    LIBRARIES Boost::unit_test_framework v::raft v::storage_test_utils v::model_test_utils
    LABELS raft
    ARGS "-- -c 8"
)

rp_test(
    UNIT_TEST
    BINARY_NAME group_configuration_tests
    SOURCES group_configuration_tests.cc
    DEFINITIONS BOOST_TEST_DYN_LINK
    LIBRARIES Boost::unit_test_framework v::raft
    LABELS raft
    ARGS "-- -c 8"
)


set(srcs
    jitter_tests.cc
    bootstrap_configuration_test.cc
    foreign_entry_test.cc
    configuration_serialization_test.cc
    type_serialization_tests.cc
    term_assigning_reader_test.cc
    membership_test.cc
    leadership_test.cc
    append_entries_test.cc
    offset_monitor_test.cc
    mux_state_machine_test.cc
    mutex_buffer_test.cc
    manual_log_deletion_test.cc
    state_removal_test.cc
    configuration_manager_test.cc
)

rp_test(
  UNIT_TEST
  BINARY_NAME test_raft
  SOURCES ${srcs}
  LIBRARIES v::seastar_testing_main v::raft v::storage_test_utils v::model_test_utils v::features
  LABELS raft
  ARGS "-- -c 8"
)

rp_test(
  UNIT_TEST
  BINARY_NAME test_offset_translator
  SOURCES offset_translator_tests.cc
  LIBRARIES v::seastar_testing_main v::raft v::storage_test_utils
  LABELS kafka
  ARGS "-- -c 8"
)
