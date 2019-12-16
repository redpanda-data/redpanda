set(request_srcs
  requests/requests.cc
  requests/api_versions_request.cc
  requests/metadata_request.cc
  requests/list_groups_request.cc
  requests/find_coordinator_request.cc
  requests/offset_fetch_request.cc
  requests/kafka_batch_adapter.cc
  requests/produce_request.cc
  requests/list_offsets_request.cc
  requests/fetch_request.cc
  requests/join_group_request.cc
  requests/heartbeat_request.cc
  requests/leave_group_request.cc
  requests/sync_group_request.cc
  requests/create_topics_request.cc)

set(group_srcs
  groups/member.cc
  groups/group.cc
  groups/group_manager.cc)

add_library(kafka STATIC
  ${request_srcs}
  ${group_srcs}
  errors.cc
  server.cc)

target_link_libraries(kafka
  PUBLIC
    Seastar::seastar
    bytes
    cluster)

add_subdirectory(tests)
