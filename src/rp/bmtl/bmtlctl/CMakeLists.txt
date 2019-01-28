add_executable(bmtlctl
  main.cc
  cli.cc
  qps_load.cc
  )
target_link_libraries (bmtlctl PUBLIC bmtl_api)
target_include_directories(bmtlctl
  PUBLIC ${CMAKE_SOURCE_DIR}/src/rp
  )
