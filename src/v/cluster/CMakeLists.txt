add_library (cluster STATIC
  metadata_cache.cc
  )
target_include_directories(cluster PUBLIC ${PROJECT_SOURCE_DIR}/src/v)

target_link_libraries(cluster
  PUBLIC
  Seastar::seastar
  rpfs
  )
