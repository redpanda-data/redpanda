rp_test(
  UNIT_TEST
  GTEST
  USE_CWD
  BINARY_NAME jumbo_log_segment_rt
  SOURCES
    jumbo_log_segment_roundtrip_test.cc
  LIBRARIES
    v::gtest_main
    v::jumbo_log_segment
    v::jumbo_log_segment_reader
    v::jumbo_log_segment_writer
    v::model_test_utils
  ARGS "-- -c1"
)
