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
    api.cc
    types.cc
    logger.cc
    script_context.cc
    script_context_frontend.cc
    script_context_backend.cc
    script_context_router.cc
    pacemaker.cc
    offset_storage_utils.cc
    wasm_event.cc
    event_listener.cc
    script_dispatcher.cc
    event_handler.cc
    partition.cc
    partition_manager.cc
  DEPS
    v::rpc
    v::model
    v::storage
    v::reflection
    v::kafka_client
    v::cluster
    coproc_gen
  )

add_subdirectory(tests)
