v_cc_library(
  NAME model
  SRCS
    model.cc
    record_batch_reader.cc
    record_utils.cc
  DEPS
    v::rphashing
    Seastar::seastar
  )
add_subdirectory(tests)
