include(rpcgen)
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
    script_context.cc
    pacemaker.cc
    offset_storage_utils.cc
    wasm_event.cc
    wasm_event_listener.cc
  DEPS
    v::rpc
    v::model
    v::storage
    v::reflection
    v::kafka_client
    coproc_engine_gen
  )

add_subdirectory(tests)
