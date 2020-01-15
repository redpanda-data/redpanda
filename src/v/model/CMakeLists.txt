v_cc_library(
  NAME model
  SRCS
    model.cc
    record_batch_reader.cc
  DEPS
    Seastar::seastar
  )
add_subdirectory(tests)
