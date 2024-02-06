v_cc_library(
  NAME transform_logging_test_utils
  HDRS
    utils.h
  SRCS
    utils.cc
  DEPS
    v::utils
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME transform_logging
  SOURCES
    model_test.cc
    log_manager_test.cc
    record_batcher_test.cc
  LIBRARIES
    v::transform_logging
    v::transform_logging_test_utils
    v::gtest_main
)
