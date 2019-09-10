find_package(Seastar)
find_package(Hdrhistogram)
add_library (utils STATIC
  hdr_hist.cc
  human.cc
  )
target_include_directories(utils PUBLIC ${PROJECT_SOURCE_DIR}/src/v)
target_link_libraries(utils PUBLIC Seastar::seastar Hdrhistogram::hdr_histogram)
add_subdirectory(tests)
