find_package(Roaring REQUIRED)

add_library (cluster STATIC
  controller.cc
  metadata_cache.cc
  partition_manager.cc
  types.cc
  partition_allocator.cc
  type_serializers.cc
  )

target_link_libraries(cluster
  PUBLIC
  Seastar::seastar
  raft
  Roaring::roaring
  )

add_subdirectory(tests)
