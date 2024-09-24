include(rpcgen)

rpcgen(TARGET generated_datalake_coordinator_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/rpc.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/rpc_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)

v_cc_library(
    NAME datalake_coordinator
    SRCS
        frontend.cc
        service.cc
        state_machine.cc
    DEPS
        generated_datalake_coordinator_rpc
        v::cluster
        v::model
        v::rpc
        Seastar::seastar
)
