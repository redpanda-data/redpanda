seastar_generate_swagger(
  TARGET  config_swagger
  VAR config_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/config.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/config.json.h
)

add_executable(redpanda
    application.cc
    main.cc
  )
target_link_libraries(redpanda PUBLIC raft transport config)
set_property(TARGET redpanda PROPERTY POSITION_INDEPENDENT_CODE ON)
add_dependencies(redpanda config_swagger)

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
