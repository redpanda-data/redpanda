rp_test(
  UNIT_TEST
  GTEST
  USE_CWD
  BINARY_NAME mvlog
  SOURCES
    batch_collector_test.cc
  LIBRARIES
    v::gtest_main
    v::model
    v::mvlog
    v::random
  ARGS "-- -c1"
)

