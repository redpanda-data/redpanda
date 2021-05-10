include(rpcgen)
rpcgen(
  TARGET coproc_gen
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/gen.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/supervisor.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
  )

file(GLOB V8_LIB_MONOLIT "v8-libs/lib/libv8_monolith.a")

v_cc_library(
  NAME coproc
  SRCS
    types.cc
    logger.cc
    script_context.cc
    pacemaker.cc
    offset_storage_utils.cc
    wasm_event.cc
    event_listener.cc
    script_dispatcher.cc
    v8-init.cc
  DEPS
    v::rpc
    v::model
    v::storage
    v::reflection
    v::kafka_client
    coproc_gen
    ${V8_LIB_MONOLIT}
    PUBLIC
  )

target_include_directories(v_coproc PUBLIC v8-libs/include)

add_subdirectory(tests)
