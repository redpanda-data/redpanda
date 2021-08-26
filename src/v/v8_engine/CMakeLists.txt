v_cc_library(
  NAME v8_engine
  SRCS
    environment.cc
    executor.cc
    script.cc
    record_batch_wrapper.cc
  DEPS
    Seastar::seastar
    v8_monolith
    v_bytes
    v_model)

add_subdirectory(tests)
