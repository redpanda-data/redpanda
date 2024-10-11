v_cc_library(
  NAME datalake_coordinator_test_utils
  HDRS
    state_test_utils.h
  SRCS
    state_test_utils.cc
  DEPS
    v::datalake_coordinator
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME coordinator
  SOURCES
    coordinator_test.cc
    state_update_test.cc
  LIBRARIES
    v::datalake_coordinator
    v::datalake_coordinator_test_utils
    v::gtest_main
    v::raft_fixture
  LABELS datalake
  ARGS "-- -c 1"
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME coordinator_iceberg
  SOURCES
    iceberg_file_committer_test.cc
  LIBRARIES
    Boost::iostreams
    v::cloud_io
    v::cloud_io_utils
    v::datalake_coordinator
    v::datalake_coordinator_test_utils
    v::gtest_main
    v::iceberg
    v::raft_fixture
    v::s3_imposter
  LABELS datalake
  ARGS "-- -c 1"
)
