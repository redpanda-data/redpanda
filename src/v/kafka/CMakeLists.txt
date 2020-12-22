add_subdirectory(requests/schemata)

set(request_srcs
  requests/requests.cc
  requests/api_versions_request.cc
  requests/metadata_request.cc
  requests/list_groups_request.cc
  requests/find_coordinator_request.cc
  requests/describe_configs_request.cc
  requests/offset_fetch_request.cc
  requests/kafka_batch_adapter.cc
  requests/produce_request.cc
  requests/list_offsets_request.cc
  requests/fetch_request.cc
  requests/join_group_request.cc
  requests/heartbeat_request.cc
  requests/leave_group_request.cc
  requests/sync_group_request.cc
  requests/init_producer_id_request.cc
  requests/create_topics_request.cc
  requests/offset_commit_request.cc
  requests/delete_topics_request.cc
  requests/alter_configs_request.cc
  requests/describe_groups_request.cc
  requests/sasl_handshake_request.cc
  requests/sasl_authenticate_request.cc
  requests/incremental_alter_configs_request.cc
  requests/topics/types.cc
  requests/topics/topic_utils.cc)

set(group_srcs
  groups/member.cc
  groups/group.cc
  groups/group_manager.cc)

set(security_srcs
    security/scram_algorithm.cc)

v_cc_library(
  NAME kafka
  SRCS
    ${request_srcs}
    ${group_srcs}
    ${security_srcs}
    errors.cc
    connection_context.cc
    protocol.cc
    protocol_utils.cc
    logger.cc
    quota_manager.cc
    fetch_session_cache.cc
 DEPS
    Seastar::seastar
    v::bytes
    v::rpc
    v::cluster
    v::kafka_request_schemata
    absl::flat_hash_map
    absl::flat_hash_set
)
add_subdirectory(tests)

set_source_files_properties(
    groups/group.cc # too big => negative impact on minimal rebuilds for unity
  PROPERTIES SKIP_UNITY_BUILD_INCLUSION 1)
