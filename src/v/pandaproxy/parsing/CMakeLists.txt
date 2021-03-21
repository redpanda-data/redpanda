v_cc_library(
  NAME pandaproxy_parsing
  SRCS
    error.cc
  DEPS
    Seastar::seastar
)

add_subdirectory(test)
