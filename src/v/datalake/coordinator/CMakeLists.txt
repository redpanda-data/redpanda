include(rpcgen)

rpcgen(TARGET generated_datalake_coordinator_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/rpc.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/rpc_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)
