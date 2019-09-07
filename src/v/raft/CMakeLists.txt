include(rpcgen)
rpcgen(
  TARGET raft_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/raftgen.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/raftgen_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
  )
add_library (raft STATIC
  service.cc
  )
target_include_directories(raft
  PUBLIC ${PROJECT_SOURCE_DIR}/src/v
  )
target_link_libraries(raft
  storage raft_rpc finjector
  )
