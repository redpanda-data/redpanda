rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME cloud_data_aggregated_uploader_gtest
  SOURCES
    aggregated_uploader_gtest.cc
  LIBRARIES
    v::gtest_main
    v::cluster
    v::storage_test_utils 
  ARGS "-- -c 2"
  LABELS cluster
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME cloud_data_aggregated_log_reader_gtest
  SOURCES
    aggregated_log_reader_gtest.cc
  LIBRARIES
    v::gtest_main
    v::cluster
    v::storage_test_utils 
  ARGS "-- -c 1"
  LABELS cluster
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME cloud_data_dl_stm_gtest
  SOURCES
    dl_stm_gtest.cc
  LIBRARIES
    v::gtest_main
    v::model
    v::raft
    v::cluster
    v::raft_fixture
    v::storage_test_utils 
  ARGS "-- -c 1"
  LABELS cluster
)