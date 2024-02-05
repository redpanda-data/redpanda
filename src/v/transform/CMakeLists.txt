v_cc_library(
  NAME transform
  HDRS
    api.h
    probe.h
    logger.h
    transform_processor.h
    transform_manager.h
    commit_batcher.h
    io.h
  SRCS
    api.cc
    probe.cc
    logger.cc
    transform_processor.cc
    transform_manager.cc
    commit_batcher.cc
    txn_reader.cc
  DEPS
    v::wasm
    v::model
    v::transform_rpc
    v::transform_logging
)

add_subdirectory(tests)
add_subdirectory(rpc)
add_subdirectory(logging)
