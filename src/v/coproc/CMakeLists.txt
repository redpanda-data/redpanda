include(rpcgen)
rpcgen(
  TARGET coproc_mgmnt_gen
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/gen.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/script_manager.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
  )

rpcgen(
  TARGET coproc_engine_gen
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/gen_engine.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/supervisor.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
  )


v_cc_library(
  NAME coproc
  SRCS
    types.cc
    logger.cc
    service.cc
    script_context.cc
    pacemaker.cc
  DEPS
    v::rpc
    v::model
    v::storage
    v::reflection
    coproc_mgmnt_gen
    coproc_engine_gen
  )

add_subdirectory(tests)
