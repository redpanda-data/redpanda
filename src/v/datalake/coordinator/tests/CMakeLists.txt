v_cc_library(
  NAME datalake_coordinator_test_utils
  HDRS
    state_test_utils.h
  SRCS
    state_test_utils.cc
  DEPS
    v::datalake_coordinator
)

rp_test(
  UNIT_TEST
  GTEST
  BINARY_NAME coordinator
  SOURCES
    state_update_test.cc
  LIBRARIES
    v::datalake_coordinator
    v::datalake_coordinator_test_utils
    v::gtest_main
  LABELS datalake
  ARGS "-- -c 1"
)

