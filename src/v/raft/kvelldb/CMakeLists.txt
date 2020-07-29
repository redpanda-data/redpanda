# server
add_executable(srvkvell
  kvserver.cc
  logger.cc
  kvrsm.cc
  )
target_link_libraries(srvkvell PUBLIC v::raft)
set_property(TARGET srvkvell PROPERTY POSITION_INDEPENDENT_CODE ON)
if(CMAKE_BUILD_TYPE MATCHES Release)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT ltosupported OUTPUT error)
  if(ltosupported)
    set_property(TARGET srvkvell PROPERTY INTERPROCEDURAL_OPTIMIZATION ON)
  endif()
endif()
