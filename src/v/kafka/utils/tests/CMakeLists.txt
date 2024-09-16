rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME
    kafka_txn_reader
  SOURCES
    txn_reader_test.cc
  LIBRARIES
    v::gtest_main
    v::model_test_utils
    v::kafka_utils
  ARGS "-- -c 1"
  LABELS kafka
)