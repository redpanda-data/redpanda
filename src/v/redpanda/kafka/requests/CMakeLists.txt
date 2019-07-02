add_library (requests STATIC
  requests.cc
  )
target_include_directories(requests PUBLIC ${PROJECT_SOURCE_DIR}/src/v)

target_link_libraries(requests
  PUBLIC
    Seastar::seastar
    bytes
    cluster
  )