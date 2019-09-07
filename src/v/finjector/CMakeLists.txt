find_package(Seastar)
add_library (finjector STATIC
  hbadger.cc
  )
target_include_directories(finjector PUBLIC ${PROJECT_SOURCE_DIR}/src/v)
target_link_libraries(finjector PUBLIC Seastar::seastar)
