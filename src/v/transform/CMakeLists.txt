v_cc_library(
  NAME transform
  HDRS
    probe.h
    logger.h
    transform_processor.h
    transform_manager.h
    io.h
  SRCS
    probe.cc
    logger.cc
    transform_processor.cc
    transform_manager.cc
  DEPS
    v::wasm
    v::model
)

add_subdirectory(tests)
add_subdirectory(rpc)
