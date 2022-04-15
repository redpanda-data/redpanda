seastar_generate_swagger(
  TARGET  config_swagger
  VAR config_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/config.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/config.json.h
)

seastar_generate_swagger(
  TARGET  cluster_config_swagger
  VAR cluster_config_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/cluster_config.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/cluster_config.json.h

)

seastar_generate_swagger(
  TARGET raft_swagger
  VAR raft_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/raft.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/raft.json.h
)

seastar_generate_swagger(
  TARGET security_swagger
  VAR security_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/security.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/security.json.h
)

seastar_generate_swagger(
  TARGET status_swagger
  VAR status_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/status.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/status.json.h
)

seastar_generate_swagger(
        TARGET features_swagger
        VAR features_swagger_file
        IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/features.json
        OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/features.json.h
)

seastar_generate_swagger(
  TARGET broker_swagger
  VAR broker_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/broker.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/broker.json.h
)

seastar_generate_swagger(
  TARGET partition_swagger
  VAR partition_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/partition.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/partition.json.h
)

seastar_generate_swagger(
  TARGET hbadger_swagger
  VAR hbadger_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/hbadger.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/hbadger.json.h
)

seastar_generate_swagger(
  TARGET transaction_swagger
  VAR transaction_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/transaction.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/transaction.json.h
)

seastar_generate_swagger(
  TARGET debug_swagger
  VAR debug_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/debug.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/debug.json.h
)
seastar_generate_swagger(
  TARGET cluster_swagger
  VAR cluster_swagger_file
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/admin/api-doc/cluster.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/admin/api-doc/cluster.json.h
)

v_cc_library(
  NAME application
  SRCS 
    admin_server.cc
    request_auth.cc
    application.cc
  DEPS
    Seastar::seastar
    v::cluster
    v::syschecks
    v::kafka
    v::coproc
    v::pandaproxy_rest
    v::pandaproxy_schema_registry
    v::archival
  )

add_executable(redpanda
    main.cc
  )
target_link_libraries(redpanda PUBLIC v::application v::raft v::kafka)
set_property(TARGET redpanda PROPERTY POSITION_INDEPENDENT_CODE ON)
add_dependencies(v_application config_swagger cluster_config_swagger raft_swagger
    security_swagger status_swagger broker_swagger partition_swagger hbadger_swagger
    transaction_swagger features_swagger debug_swagger cluster_swagger)

if(CMAKE_BUILD_TYPE MATCHES Release)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT ltosupported OUTPUT error)
  if(ltosupported)
    set_property(TARGET redpanda PROPERTY INTERPROCEDURAL_OPTIMIZATION ON)
  endif()
endif()
install(TARGETS redpanda DESTINATION bin)
