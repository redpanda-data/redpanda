v_cc_library(
  NAME pandaproxy_json
  SRCS
    error.cc
  DEPS
    Seastar::seastar
)

add_subdirectory(test)
add_subdirectory(requests)
