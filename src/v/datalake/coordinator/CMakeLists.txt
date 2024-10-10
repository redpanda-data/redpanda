include(rpcgen)

rpcgen(TARGET generated_datalake_coordinator_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/rpc.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/rpc_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)

v_cc_library(
    NAME datalake_coordinator
    SRCS
        coordinator.cc
        data_file.cc
        frontend.cc
        service.cc
        state.cc
        state_machine.cc
        state_update.cc
        translated_offset_range.cc
    DEPS
        generated_datalake_coordinator_rpc
        v::cluster
        v::model
        v::rpc
        Seastar::seastar
)

add_subdirectory(tests)
