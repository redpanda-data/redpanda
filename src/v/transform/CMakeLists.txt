v_cc_library(
  NAME transform
  HDRS
    api.h
    probe.h
    logger.h
    transform_processor.h
    transform_manager.h
    io.h
  SRCS
    api.cc
    probe.cc
    logger.cc
    transform_processor.cc
    transform_manager.cc
  DEPS
    v::wasm
    v::model
    v::transform_rpc
)

add_subdirectory(tests)
add_subdirectory(rpc)
