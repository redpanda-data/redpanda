find_package(Roaring REQUIRED)
include(rpcgen)
rpcgen(
  TARGET bootstrap_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/cluster_bootstrap.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/cluster_bootstrap_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)
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

rpcgen(
  TARGET partition_balancer_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/partition_balancer_rpc.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/partition_balancer_rpc_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)

rpcgen(
  TARGET node_status_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/node_status_rpc.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/node_status_rpc_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)

rpcgen(
  TARGET ephemeral_credential_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/ephemeral_credential_rpc.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/ephemeral_credential_rpc_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)

rpcgen(
  TARGET self_test_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/self_test_rpc.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/self_test_rpc_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)

rpcgen(
  TARGET topic_recovery_status_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/topic_recovery_status_rpc.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/topic_recovery_status_rpc_service.h
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
    types.cc
    notification_latch.cc
    topic_table.cc
    topic_table_probe.cc
    topic_updates_dispatcher.cc
    members_table.cc
    members_manager.cc
    partition_leaders_table.cc
    topics_frontend.cc
    controller_backend.cc
    controller_probe.cc
    controller_log_limiter.cc
    controller_stm.cc
    controller_snapshot.cc
    controller.cc
    partition.cc
    partition_probe.cc
    id_allocator_stm.cc
    persisted_stm.cc
    tm_stm_cache.cc
    tm_stm.cc
    rm_stm.cc
    tx_helpers.cc
    security_manager.cc
    security_frontend.cc
    cluster_discovery.cc
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
    scheduling/leader_balancer_constraints.cc
    health_monitor_types.cc
    health_monitor_backend.cc
    health_monitor_frontend.cc
    metrics_reporter.cc
    node/types.cc
    node/local_monitor.cc
    feature_backend.cc
    feature_manager.cc
    feature_barrier.cc
    drain_manager.cc
    remote_topic_configuration_source.cc
    partition_balancer_planner.cc
    partition_balancer_backend.cc
    partition_balancer_state.cc
    partition_balancer_rpc_handler.cc
    node_status_backend.cc
    node_status_rpc_handler.cc
    self_test_backend.cc
    self_test_frontend.cc
    self_test_rpc_handler.cc
    self_test_rpc_types.cc
    self_test/diskcheck.cc
    self_test/netcheck.cc
    bootstrap_service.cc
    bootstrap_backend.cc
    ephemeral_credential_frontend.cc
    ephemeral_credential_service.cc
    topic_recovery_status_rpc_handler.cc
    topic_recovery_status_frontend.cc
    node_isolation_watcher.cc
    topic_recovery_status_types.cc
    topic_table_partition_generator.cc
    cloud_storage_size_reducer.cc
    topic_recovery_service.cc
    partition_recovery_manager.cc
    ../archival/archival_policy.cc
    ../archival/ntp_archiver_service.cc
    ../archival/probe.cc
    ../archival/types.cc
    ../archival/upload_controller.cc
    ../archival/segment_reupload.cc
    ../archival/retention_calculator.cc
    ../archival/upload_housekeeping_service.cc
    ../archival/adjacent_segment_merger.cc
  DEPS
    Seastar::seastar
    bootstrap_rpc
    controller_rpc
    metadata_rpc
    id_allocator_rpc
    tx_gateway_rpc
    partition_balancer_rpc
    node_status_rpc
    ephemeral_credential_rpc
    self_test_rpc
    topic_recovery_status_rpc
    v::raft
    Roaring::roaring
    absl::flat_hash_map
    v::model
    v::cloud_storage
    v::features
  )
add_subdirectory(tests)
add_subdirectory(self_test/tests)
