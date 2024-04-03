v_cc_library(
  NAME transform
  SRCS
    api.cc
    probe.cc
    logger.cc
    transform_processor.cc
    transform_manager.cc
    commit_batcher.cc
    txn_reader.cc
    transform_offsets_stm.cc
  DEPS
    v::wasm
    v::model
    v::transform_rpc
    v::transform_logging
)

add_subdirectory(tests)
add_subdirectory(rpc)
add_subdirectory(logging)
