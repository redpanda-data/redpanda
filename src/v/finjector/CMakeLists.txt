find_package(Seastar)
add_library (finjector STATIC
  hbadger.cc
  )
target_link_libraries(finjector PUBLIC Seastar::seastar)
