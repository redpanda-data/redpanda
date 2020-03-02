find_package(Roaring REQUIRED)
include(rpcgen)
rpcgen(
  TARGET controller_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/controller.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/controller_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)

v_cc_library(
  NAME cluster
  SRCS
    controller.cc
    metadata_cache.cc
    partition_manager.cc
    types.cc
    partition_allocator.cc
    logger.cc
    cluster_utils.cc
    service.cc
  DEPS
    Seastar::seastar
    controller_rpc
    v::raft
    Roaring::roaring
    absl::flat_hash_map
  )
add_subdirectory(tests)
