add_library (transport STATIC
  server.cc
  )
target_include_directories(transport INTERFACE ${PROJECT_SOURCE_DIR}/src/v)

target_link_libraries(transport
  PUBLIC
    Seastar::seastar
    requests
    cluster
  )