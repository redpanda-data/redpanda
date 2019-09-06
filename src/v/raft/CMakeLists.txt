add_library (raft STATIC
  )
target_include_directories(raft
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
  )
target_link_libraries(raft
  rpfs
  )
