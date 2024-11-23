v_cc_library(
  NAME kd_test_utils
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
  BINARY_NAME test_kd_record_batcher
  SOURCES
    record_batcher_test.cc
  LIBRARIES
    v::gtest_main
    v::kd_record_batcher
    v::kd_test_utils
  LABELS kafka
)
