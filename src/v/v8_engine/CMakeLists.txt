add_subdirectory(internal)

v_cc_library(
  NAME v8_engine
  SRCS
    data_policy.cc
    data_policy_table.cc
  DEPS
    Seastar::seastar
    v::v8_engine_internal
    v::reflection)
