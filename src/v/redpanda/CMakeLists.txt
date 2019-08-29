add_executable(redpanda
  main.cc
  )
target_link_libraries(redpanda PUBLIC raft transport)
set_property(TARGET redpanda PROPERTY POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE MATCHES Debug)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT ltosupported OUTPUT error)
  if(ltosupported)
    set_property(TARGET redpanda PROPERTY INTERPROCEDURAL_OPTIMIZATION ON)
  else()
    message(STATUS "IPO/LTO not supported: ${error}")
  endif()
endif()
install(TARGETS redpanda DESTINATION bin)
add_subdirectory(kafka)
add_subdirectory(config)
