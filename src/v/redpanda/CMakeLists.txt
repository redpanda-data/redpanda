seastar_generate_swagger(
  TARGET  config_swagger
  VAR config_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/config.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/config.json.h
)

seastar_generate_swagger(
  TARGET raft_swagger
  VAR raft_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/raft.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/raft.json.h
)

seastar_generate_swagger(
  TARGET kafka_swagger
  VAR kafka_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/kafka.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/kafka.json.h
)

seastar_generate_swagger(
  TARGET partition_swagger
  VAR partition_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/partition.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/partition.json.h
)

seastar_generate_swagger(
  TARGET security_swagger
  VAR security_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/security.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/security.json.h
)

v_cc_library(
  NAME application
  SRCS application.cc
  DEPS
    Seastar::seastar
    v::cluster
    v::syschecks
    v::kafka
    v::coproc
    v::pandaproxy
    v::archival
  )

add_executable(redpanda
    main.cc
  )
target_link_libraries(redpanda PUBLIC v::application v::raft v::kafka)
set_property(TARGET redpanda PROPERTY POSITION_INDEPENDENT_CODE ON)
add_dependencies(v_application config_swagger raft_swagger kafka_swagger
    partition_swagger security_swagger)

if(CMAKE_BUILD_TYPE MATCHES Release)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT ltosupported OUTPUT error)
  if(ltosupported)
    set_property(TARGET redpanda PROPERTY INTERPROCEDURAL_OPTIMIZATION ON)
  endif()
endif()
install(TARGETS redpanda DESTINATION bin)
