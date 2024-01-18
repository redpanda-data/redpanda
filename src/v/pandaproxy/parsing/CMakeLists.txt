v_cc_library(
  NAME pandaproxy_parsing
  SRCS
    error.cc
  DEPS
    absl::btree
    Seastar::seastar
    v::strings
)

add_subdirectory(test)
