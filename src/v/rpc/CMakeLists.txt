find_package(Seastar)
add_library (rpc STATIC
  types.cc
  netbuf.cc
  server.cc
  client.cc
  )
target_include_directories(rpc PUBLIC ${PROJECT_SOURCE_DIR}/src/v)
target_link_libraries(rpc PUBLIC Seastar::seastar bytes rphashing)
add_subdirectory(test)
