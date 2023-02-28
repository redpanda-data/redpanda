set(swags
  config
  cluster_config
  raft
  security
  status
  features
  broker
  partition
  hbadger
  transaction
  cluster
  debug
  shadow_indexing
  redpanda_services_restart)

set(swag_files)
foreach(swag ${swags})
  seastar_generate_swagger(
    TARGET ${swag}_swagger
    VAR ${swag}_swagger_files
    IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/api-doc/${swag}.json
    OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/api-doc/${swag}.json.h
  )
  list(APPEND swag_files ${${swag}_swagger_files})
endforeach()

v_cc_library(
  NAME admin
  SRCS
    ${swag_files}
    admin_server.cc
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
