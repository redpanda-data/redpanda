add_library (bytes STATIC
  bytes.cc
  )

target_link_libraries(bytes
  PUBLIC
  Seastar::seastar
  )

add_subdirectory(tests)
