include(rpcgen)
rpcgen(
  TARGET coproc_gen
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/gen.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/liaison.h
  INCLUDES {$CMAKE_BINARY_DIR}/src/v
  )

v_cc_library(
  NAME coproc
  SRCS
    logger.cc
  DEPS
    v::rpc
    v::model
    coproc_gen
  )
