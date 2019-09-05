add_library (requests STATIC
  requests.cc
  metadata_request.cc
  list_groups_request.cc
  find_coordinator_request.cc
  offset_fetch_request.cc
  )
target_include_directories(requests PUBLIC ${PROJECT_SOURCE_DIR}/src/v)

target_link_libraries(requests
  PUBLIC
    Seastar::seastar
    bytes
    cluster
  )
