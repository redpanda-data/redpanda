set (srcs
    state_machine_test.cc
)

rp_test(
  UNIT_TEST
  GTEST
  TIMEOUT 2000
  BINARY_NAME datalake_translation_tests
  SOURCES ${srcs}
  LIBRARIES  v::datalake_translation v::features v::raft_fixture v::gtest_main
  LABELS datalake
  ARGS "-- -c 1 -m 2G"
)

rp_test(
  UNIT_TEST
  GTEST
  TIMEOUT 2000
  BINARY_NAME translated_log_offset_tests
  SOURCES translated_log_offset_test.cc
  LIBRARIES  v::datalake_translation v::features v::raft_fixture v::gtest_main v::storage v::storage_test_utils
  LABELS datalake
  ARGS "-- -c 1"
)
