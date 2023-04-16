add_subdirectory(protocol)

set(handlers_srcs
  server/handlers/api_versions.cc
  server/handlers/metadata.cc
  server/handlers/txn_offset_commit.cc
  server/handlers/find_coordinator.cc
  server/handlers/describe_configs.cc
  server/handlers/produce.cc
  server/handlers/list_offsets.cc
  server/handlers/fetch.cc
  server/handlers/create_topics.cc
  server/handlers/alter_configs.cc
  server/handlers/incremental_alter_configs.cc
  server/handlers/describe_acls.cc
  server/handlers/describe_log_dirs.cc
  server/handlers/delete_acls.cc
  server/handlers/create_partitions.cc
  server/handlers/offset_for_leader_epoch.cc
  server/handlers/alter_partition_reassignments.cc
  server/handlers/list_partition_reassignments.cc
  server/handlers/handler_interface.cc
  server/handlers/topics/types.cc
  server/handlers/topics/topic_utils.cc
  server/handlers/describe_producers.cc
  server/handlers/describe_transactions.cc
)

v_cc_library(
  NAME kafka
  SRCS
    ${handlers_srcs}
    server/requests.cc
    server/member.cc
    server/group_stm.cc
    server/group.cc
    server/group_router.cc
    server/group_manager.cc
    server/usage_manager.cc
    server/rm_group_frontend.cc
    server/connection_context.cc
    server/server.cc
    server/protocol_utils.cc
    server/quota_manager.cc
    server/snc_quota_manager.cc
    server/fetch_session_cache.cc
    server/replicated_partition.cc
    server/partition_proxy.cc
    server/group_recovery_consumer.cc
    server/group_metadata.cc
 DEPS
    Seastar::seastar
    v::bytes
    v::rpc
    v::coproc
    v::cluster
    v::kafka_protocol
    v::security
    absl::flat_hash_map
    absl::flat_hash_set
)

add_subdirectory(client)
add_subdirectory(server/tests)

set_source_files_properties(
    server/group.cc # too big => negative impact on minimal rebuilds for unity
  PROPERTIES SKIP_UNITY_BUILD_INCLUSION 1)
