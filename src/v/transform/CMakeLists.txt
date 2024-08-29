v_cc_library(
  NAME transform
  SRCS
    api.cc
    probe.cc
    logger.cc
    transform_processor.cc
    transform_manager.cc
    commit_batcher.cc
    memory_limiter.cc
  DEPS
    v::wasm
    v::model
    v::kafka_utils
    v::transform_rpc
    v::transform_logging
    v::transform_stm
)

add_subdirectory(tests)
add_subdirectory(rpc)
add_subdirectory(logging)
add_subdirectory(stm)
