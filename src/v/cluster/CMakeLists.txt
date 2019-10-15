add_library (cluster STATIC
  controller.cc
  metadata_cache.cc
  partition_manager.cc
  )
target_include_directories(cluster PUBLIC ${PROJECT_SOURCE_DIR}/src/v)

target_link_libraries(cluster
  PUBLIC
  Seastar::seastar
  raft
  )
