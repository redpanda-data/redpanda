include(rpcgen)
rpcgen(
  TARGET raft_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/raftgen.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/raftgen_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
  )

v_cc_library(
  NAME raft
  SRCS
    consensus.cc
    consensus_utils.cc
    heartbeat_manager.cc
    configuration_bootstrap_state.cc
    logger.cc
    types.cc
    replicate_entries_stm.cc
    vote_stm.cc
    recovery_stm.cc
    follower_stats.cc
    replicate_batcher.cc
    rpc_client_protocol.cc
  DEPS
    v::storage
    raft_rpc
    v::finjector
  )
add_subdirectory(tests)
add_subdirectory(tron)
