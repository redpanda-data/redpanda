include(smfc_generator)
smfc_gen(
  CPP
  TARGET_NAME raft_fbs
  OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  SOURCES raft.fbs)


add_library (raft STATIC
  ${raft_fbs}
  raft_service.cc
  )
target_include_directories(raft
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
  )
target_link_libraries(raft
  rpfs
  )
