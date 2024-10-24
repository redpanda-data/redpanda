rp_test(
  UNIT_TEST
  GTEST
  USE_CWD
  BINARY_NAME mvlog
  SOURCES
    active_segment_test.cc
    batch_collecting_stream_utils_test.cc
    batch_collector_test.cc
    entry_stream_utils_test.cc
    file_test.cc
    segment_appender_test.cc
    segment_io_test.cc
    segment_reader_test.cc
    skipping_data_source_test.cc
  LIBRARIES
    v::gtest_main
    v::hashing
    v::model
    v::model_test_utils
    v::mvlog
    v::random
    v::storage
  ARGS "-- -c1"
)

