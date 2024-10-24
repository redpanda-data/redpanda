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
