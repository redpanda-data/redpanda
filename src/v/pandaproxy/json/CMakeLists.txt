v_cc_library(
  NAME pandaproxy_json
  SRCS
    error.cc
  DEPS
    Seastar::seastar
    v::pandaproxy_common
)

add_subdirectory(test)
add_subdirectory(requests)
