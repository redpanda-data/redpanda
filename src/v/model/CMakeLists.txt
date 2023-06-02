v_cc_library(
  NAME model
  SRCS
    adl_serde.cc
    async_adl_serde.cc
    model_socheck.cc
    model.cc
    record_batch_reader.cc
    record_utils.cc
    validation.cc
  DEPS
    v::bytes
    v::rphashing
    v::reflection
    Seastar::seastar
  )
add_subdirectory(tests)
