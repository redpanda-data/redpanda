add_executable(demo_server server.cc)
target_link_libraries(demo_server PUBLIC rpc)
set_property(TARGET demo_server PROPERTY POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE MATCHES Debug)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT ltosupported OUTPUT error)
  if(ltosupported)
    set_property(TARGET demo_server PROPERTY INTERPROCEDURAL_OPTIMIZATION ON)
  else()
    message(STATUS "IPO/LTO not supported: ${error}")
  endif()
endif()

# client
add_executable(demo_client client.cc)
target_link_libraries(demo_client PUBLIC rpc)
set_property(TARGET demo_client PROPERTY POSITION_INDEPENDENT_CODE ON)
