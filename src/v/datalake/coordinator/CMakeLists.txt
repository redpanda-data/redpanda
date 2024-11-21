include(rpcgen)

rpcgen(TARGET generated_datalake_coordinator_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/rpc.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/rpc_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)

v_cc_library(
    NAME datalake_coordinator
    SRCS
        commit_offset_metadata.cc
        coordinator.cc
        coordinator_manager.cc
        data_file.cc
        iceberg_file_committer.cc
        frontend.cc
        service.cc
        state.cc
        state_machine.cc
        state_update.cc
        translated_offset_range.cc
        types.cc
        catalog_factory.cc
    DEPS
        generated_datalake_coordinator_rpc
        v::cluster
        v::datalake_common
        v::datalake_writer
        v::model
        v::rpc
        Seastar::seastar
)

add_subdirectory(tests)
