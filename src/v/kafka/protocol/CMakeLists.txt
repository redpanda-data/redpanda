add_subdirectory(schemata)

v_cc_library(
  NAME kafka_protocol
  SRCS
    errors.cc
  DEPS
    v::kafka_request_schemata
    v::pandaproxy_common)

add_subdirectory(tests)
