if ("${REDPANDA_ENABLE_V8}" MATCHES "TRUE")
  add_subdirectory(internal)
endif()

v_cc_library(
  NAME v8_engine
  SRCS
    data_policy.cc
    data_policy_table.cc
  DEPS
    Seastar::seastar
    v::reflection)
