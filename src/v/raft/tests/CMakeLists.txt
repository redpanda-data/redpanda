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
    mutex_buffer_test.cc
    state_removal_test.cc
    configuration_manager_test.cc
    coordinated_recovery_throttle_test.cc
    heartbeats_test.cc
)

rp_test(
  UNIT_TEST
  BINARY_NAME test_raft
  SOURCES ${srcs}
  LIBRARIES v::seastar_testing_main v::raft v::storage_test_utils v::model_test_utils v::features v::cluster
  LABELS raft
  ARGS "-- -c 8"
)

set(gsrcs
    raft_fixture.cc
    basic_raft_fixture_test.cc
    stm_manager_test.cc
    raft_reconfiguration_test.cc
    persisted_stm_test.cc
    replication_monitor_tests.cc
    mux_state_machine_test.cc
)

rp_test(
  UNIT_TEST
  GTEST
  TIMEOUT 2000
  BINARY_NAME gtest_raft
  SOURCES ${gsrcs}
  LIBRARIES  v::raft v::storage_test_utils v::model_test_utils v::features v::gtest_main
  LABELS raft
  ARGS "-- -c 8 -m 2G"
)

rp_test(
  BENCHMARK_TEST
  BINARY_NAME heartbeat_bench
  SOURCES heartbeat_bench.cc
  LIBRARIES Seastar::seastar_perf_testing Boost::unit_test_framework v::raft
  # the args below are just to keep it fast
  ARGS "-c 1 --duration=1 --runs=1 --memory=1G"
  LABELS raft
)

v_cc_library(
    NAME raft_fixture
    SRCS raft_fixture.cc
    DEPS Seastar::seastar v::raft v::gtest_main)
