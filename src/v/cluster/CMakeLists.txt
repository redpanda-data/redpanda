find_package(Roaring REQUIRED)
include(rpcgen)
rpcgen(
  TARGET controller_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/controller.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/controller_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)

rpcgen(
  TARGET metadata_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/metadata_dissemination_rpc.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/metadata_dissemination_rpc_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)

rpcgen(
  TARGET id_allocator_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/id_allocator.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/id_allocator_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)

rpcgen(
  TARGET tx_gateway_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/tx_gateway.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/tx_gateway_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)

v_cc_library(
  NAME cluster
  SRCS
    archival_metadata_stm.cc
    metadata_cache.cc
    partition_manager.cc
    scheduling/partition_allocator.cc
    logger.cc
    config_frontend.cc
    config_manager.cc
    cluster_utils.cc
    id_allocator.cc
    id_allocator_frontend.cc
    rm_partition_frontend.cc
    tx_gateway_frontend.cc
    tx_gateway.cc
    service.cc
    metadata_dissemination_handler.cc
    metadata_dissemination_service.cc
    metadata_dissemination_utils.cc
    types.cc
    notification_latch.cc
    topic_table.cc
    topic_updates_dispatcher.cc
    members_table.cc
    members_manager.cc
    partition_leaders_table.cc
    topics_frontend.cc
    controller_backend.cc
    controller.cc
    partition.cc
    partition_probe.cc
    id_allocator_stm.cc
    persisted_stm.cc
    tm_stm.cc
    rm_stm.cc
    tx_helpers.cc
    security_manager.cc
    security_frontend.cc
    data_policy_manager.cc
    data_policy_frontend.cc
    controller_api.cc
    members_frontend.cc
    members_backend.cc
    health_manager.cc
    non_replicable_topics_frontend.cc
    scheduling/allocation_node.cc
    scheduling/types.cc
    scheduling/allocation_state.cc
    scheduling/allocation_strategy.cc
    scheduling/constraints.cc
    scheduling/leader_balancer.cc
    scheduling/leader_balancer_probe.cc
    health_monitor_types.cc
    health_monitor_backend.cc
    health_monitor_frontend.cc
    metrics_reporter.cc
    node/types.cc
    node/local_monitor.cc
    feature_backend.cc
    feature_manager.cc
    feature_barrier.cc
    feature_table.cc
  DEPS
    Seastar::seastar
    controller_rpc
    metadata_rpc
    id_allocator_rpc
    tx_gateway_rpc
    v::raft
    Roaring::roaring
    absl::flat_hash_map
    v::model
    v::v8_engine
    v::cloud_storage
  )
add_subdirectory(tests)
