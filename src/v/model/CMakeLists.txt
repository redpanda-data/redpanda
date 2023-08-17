v_cc_library(
  NAME model
  SRCS
    model.cc
    record_batch_reader.cc
    record_utils.cc
    async_adl_serde.cc
    adl_serde.cc
    validation.cc
    transform.cc
  DEPS
    v::bytes
    v::utils
    v::rphashing
    v::reflection
    Seastar::seastar
  )
add_subdirectory(tests)
