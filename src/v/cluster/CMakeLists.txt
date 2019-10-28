add_library (cluster STATIC
  controller.cc
  metadata_cache.cc
  partition_manager.cc
  types.cc
  partition_allocator.cc
  type_serializers.cc
  simple_batch_builder.cc
  )
target_include_directories(cluster PUBLIC ${PROJECT_SOURCE_DIR}/src/v)

target_link_libraries(cluster
  PUBLIC
  Seastar::seastar
  raft
  Roaring::roaring
  )

add_subdirectory(tests)
