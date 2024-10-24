v_cc_library(
  NAME schema
  SRCS
    registry.cc
  DEPS
    v::pandaproxy_schema_registry
    Seastar::seastar
)

add_subdirectory(tests)
