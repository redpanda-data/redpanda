v_cc_library(
  NAME v8_engine
  SRCS
    environment.cc
    executor.cc
    record_batch_wrapper.cc
    script.cc
  DEPS
    Seastar::seastar
    v8_monolith
    v::model
    v::coproc)

add_subdirectory(tests)
