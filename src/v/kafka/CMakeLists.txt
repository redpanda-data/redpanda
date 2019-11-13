add_subdirectory(requests)
add_subdirectory(groups)

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

add_library (errors STATIC
    errors.cc)

target_link_libraries(errors
    PUBLIC
      Seastar::seastar
      cluster
    )
add_subdirectory(tests)
