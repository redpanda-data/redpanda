add_subdirectory(protocol)

set(handlers_srcs
  server/handlers/api_versions.cc
  server/handlers/metadata.cc
  server/handlers/txn_offset_commit.cc
  server/handlers/list_groups.cc
  server/handlers/find_coordinator.cc
  server/handlers/describe_configs.cc
  server/handlers/offset_fetch.cc
  server/handlers/produce.cc
  server/handlers/list_offsets.cc
  server/handlers/fetch.cc
  server/handlers/join_group.cc
  server/handlers/heartbeat.cc
  server/handlers/leave_group.cc
  server/handlers/sync_group.cc
  server/handlers/init_producer_id.cc
  server/handlers/create_topics.cc
  server/handlers/offset_commit.cc
  server/handlers/delete_topics.cc
  server/handlers/alter_configs.cc
  server/handlers/describe_groups.cc
  server/handlers/sasl_handshake.cc
  server/handlers/sasl_authenticate.cc
  server/handlers/incremental_alter_configs.cc
  server/handlers/delete_groups.cc
  server/handlers/describe_acls.cc
  server/handlers/describe_log_dirs.cc
  server/handlers/create_acls.cc
  server/handlers/delete_acls.cc
  server/handlers/topics/types.cc
  server/handlers/topics/topic_utils.cc
)

v_cc_library(
  NAME kafka
  SRCS
    protocol/batch_reader.cc
    protocol/kafka_batch_adapter.cc
    ${handlers_srcs}
    server/requests.cc
    server/member.cc
    server/group_stm.cc
    server/group.cc
    server/group_router.cc
    server/group_manager.cc
    server/connection_context.cc
    server/protocol.cc
    server/protocol_utils.cc
    server/logger.cc
    server/quota_manager.cc
    server/fetch_session_cache.cc
 DEPS
    Seastar::seastar
    v::bytes
    v::rpc
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
