include(rpcgen)
rpcgen(
  TARGET demo_gen
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/gen.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/simple_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
  )

add_executable(demo_server server.cc)
target_link_libraries(demo_server PUBLIC demo_gen)
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
target_link_libraries(demo_client PUBLIC demo_gen)
set_property(TARGET demo_client PROPERTY POSITION_INDEPENDENT_CODE ON)


rp_test(
  UNIT_TEST
  BINARY_NAME roundtrip_demo_types
  SOURCES type_tests.cc
  LIBRARIES Seastar::seastar_testing demo_gen
  ARGS "-- -c 1"
)
