add_subdirectory(schemata)

v_cc_library(
  NAME kafka_protocol
  SRCS
    errors.cc
  DEPS
    v::kafka_request_schemata)

add_subdirectory(tests)
