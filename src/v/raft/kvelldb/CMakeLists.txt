# server
add_executable(kvelldb
  kvserver.cc
  logger.cc
  kvrsm.cc
  httpkvrsm.cc
  )
target_link_libraries(kvelldb PUBLIC v::raft v::features)
set_property(TARGET kvelldb PROPERTY POSITION_INDEPENDENT_CODE ON)
if(CMAKE_BUILD_TYPE MATCHES Release)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT ltosupported OUTPUT error)
  if(ltosupported)
    set_property(TARGET kvelldb PROPERTY INTERPROCEDURAL_OPTIMIZATION ON)
  endif()
endif()
