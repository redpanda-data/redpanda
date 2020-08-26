v_cc_library(
  NAME model
  SRCS
    model.cc
    record_batch_reader.cc
    record_utils.cc
    adl_serde.cc
    validation.cc
  DEPS
    v::bytes
    v::rphashing
    v::reflection
    Seastar::seastar
  )
add_subdirectory(tests)
