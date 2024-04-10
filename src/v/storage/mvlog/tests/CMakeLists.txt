rp_test(
  UNIT_TEST
  GTEST
  USE_CWD
  BINARY_NAME mvlog
  SOURCES
    batch_collector_test.cc
    entry_stream_utils_test.cc
    segment_appender_test.cc
    skipping_data_source_test.cc
  LIBRARIES
    v::gtest_main
    v::model
    v::model_test_utils
    v::mvlog
    v::random
  ARGS "-- -c1"
)

