v_cc_library(
  NAME kd_rpc_test_deps
  HDRS
    deps.h
  SRCS
    deps.cc
  DEPS
    v::kd_rpc
)

rp_test(
  FIXTURE_TEST
  GTEST
  BINARY_NAME
    kd_rpc
  SOURCES
    kafka_data_rpc_test.cc
  LIBRARIES
    v::gtest_main
    v::kd_rpc_test_deps
    v::model_test_utils
)
