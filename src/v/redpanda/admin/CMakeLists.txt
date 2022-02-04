set(swags
  config
  cluster_config
  raft
  security
  status
  broker
  partition
  hbadger)

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
  NAME admin_server
  SRCS
    server.cc
    hbadger.cc
    partition.cc
    broker.cc
    security.cc
    raft.cc
    cluster_config.cc
    ${swag_files}
 DEPS
    Seastar::seastar
    v::cluster
)
