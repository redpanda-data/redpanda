find_package(Hdrhistogram)
add_library (utils STATIC
  hdr_hist.cc
  human.cc
  )
target_link_libraries(utils PUBLIC Seastar::seastar Hdrhistogram::hdr_histogram)
add_subdirectory(tests)
