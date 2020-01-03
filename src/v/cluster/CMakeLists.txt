find_package(Roaring REQUIRED)
include(rpcgen)
rpcgen(
  TARGET controller_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/controller.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/controller_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)

add_library (cluster STATIC
  controller.cc
  metadata_cache.cc
  partition_manager.cc
  types.cc
  partition_allocator.cc
  type_serializers.cc
  logger.cc
  cluster_utils.cc
  service.cc
  )

target_link_libraries(cluster
  PUBLIC
  Seastar::seastar
  controller_rpc
  raft
  Roaring::roaring
  )

add_subdirectory(tests)
