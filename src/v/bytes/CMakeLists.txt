add_library (bytes STATIC
  bytes.cc
  )
target_include_directories(bytes PUBLIC ${PROJECT_SOURCE_DIR}/src/v)

target_link_libraries(bytes
  PUBLIC
  Seastar::seastar
  )

add_subdirectory(tests)
