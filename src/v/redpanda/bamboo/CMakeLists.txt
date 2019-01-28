add_executable(bamboo
  main.cc
  cli.cc
  qps_load.cc
  )
target_link_libraries (bamboo PUBLIC redpanda_api)
target_include_directories(bamboo
  PUBLIC ${CMAKE_SOURCE_DIR}/src/v)
