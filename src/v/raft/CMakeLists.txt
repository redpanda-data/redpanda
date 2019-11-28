include(rpcgen)
rpcgen(
  TARGET raft_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/raftgen.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/raftgen_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
  )
add_library (raft STATIC
  type_serializers.cc
  consensus.cc
  reconnect_client.cc
  client_cache.cc
  consensus_utils.cc
  heartbeat_manager.cc
  heartbeat_manager_utils.cc
  configuration_bootstrap_state.cc
  logger.cc
  types.cc
  replicate_entries_stm.cc
  )

target_link_libraries(raft
  storage raft_rpc finjector
  )



add_subdirectory(tests)
add_subdirectory(tron)
