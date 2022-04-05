v_cc_library(
  NAME v8_engine_internal
  SRCS
    environment.cc
    executor.cc
    script.cc
  DEPS
    Seastar::seastar
    v8_monolith)

add_subdirectory(tests)
