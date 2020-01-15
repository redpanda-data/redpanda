seastar_generate_swagger(
  TARGET  config_swagger
  VAR config_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/config.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/config.json.h
)

v_cc_library(
  NAME application
  SRCS application.cc
  DEPS
    Seastar::seastar
    v::cluster
    v::syschecks
    v::kafka
  )

add_executable(redpanda
    main.cc
  )
target_link_libraries(redpanda PUBLIC v::application v::raft v::kafka)
set_property(TARGET redpanda PROPERTY POSITION_INDEPENDENT_CODE ON)
add_dependencies(v_application config_swagger)

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
