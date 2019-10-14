add_library (requests STATIC
  requests.cc
  metadata_request.cc
  list_groups_request.cc
  find_coordinator_request.cc
  offset_fetch_request.cc
  kafka_batch_adapter.cc
  produce_request.cc
  list_offsets_request.cc
  fetch_request.cc
  join_group_request.cc
  heartbeat_request.cc
  leave_group_request.cc
  sync_group_request.cc
  )
target_include_directories(requests PUBLIC ${PROJECT_SOURCE_DIR}/src/v)

target_link_libraries(requests
  PUBLIC
    Seastar::seastar
    bytes
    cluster
    model
    storage
    errors
  )

add_subdirectory(tests)
