rpcgen(
  TARGET tron_gen
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/gen.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/trongen_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
  )
# server
add_executable(srvtron
  server.cc
  logger.cc
  )
target_link_libraries(srvtron PUBLIC tron_gen v::raft)
set_property(TARGET srvtron PROPERTY POSITION_INDEPENDENT_CODE ON)
if(NOT CMAKE_BUILD_TYPE MATCHES Debug)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT ltosupported OUTPUT error)
  if(ltosupported)
    set_property(TARGET srvtron PROPERTY INTERPROCEDURAL_OPTIMIZATION ON)
  else()
    message(STATUS "IPO/LTO not supported: ${error}")
  endif()
endif()
# client
add_executable(tron
  client.cc
  logger.cc
  )
target_link_libraries(tron PUBLIC tron_gen v::raft)
set_property(TARGET tron PROPERTY POSITION_INDEPENDENT_CODE ON)
