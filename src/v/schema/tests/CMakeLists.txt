v_cc_library(
  NAME schema_test_fixture
  HDRS
    fake_registry.h
  SRCS
    "fake_registry.cc"
  DEPS
    Seastar::seastar
    v::schema
)

rp_test (
  UNIT_TEST
  GTEST
  BINARY_NAME schema_utils
  SOURCES utils_test.cc
  LABELS schema
  LIBRARIES
    v::gtest_main
    v::schema
  ARGS "-- -c 1"
)
