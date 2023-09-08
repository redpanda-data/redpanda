rp_test(
  FIXTURE_TEST
  GTEST
  BINARY_NAME
    transform_rpc
  SOURCES
    transform_rpc_test.cc
  LIBRARIES 
    v::gtest_main
    v::transform_rpc
    v::model_test_utils
  ARGS "-- -c 1"
  LABELS transform_rpc
)
