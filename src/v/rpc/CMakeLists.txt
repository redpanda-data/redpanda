find_package(Seastar)
add_library (rpc INTERFACE
  )
target_include_directories(rpc INTERFACE ${PROJECT_SOURCE_DIR}/src/v)
target_link_libraries(rpc INTERFACE Seastar::seastar)
add_subdirectory(test)
