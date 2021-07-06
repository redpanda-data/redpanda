include(rpcgen)
rpcgen(
  TARGET coproc_gen
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/gen.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/supervisor.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
  )

v_cc_library(
  NAME coproc
  SRCS
    types.cc
    logger.cc
    script_context.cc
    script_context_frontend.cc
    pacemaker.cc
    offset_storage_utils.cc
    wasm_event.cc
    event_listener.cc
    script_dispatcher.cc
  DEPS
    v::rpc
    v::model
    v::storage
    v::reflection
    v::kafka_client
    coproc_gen
  )

add_subdirectory(tests)
