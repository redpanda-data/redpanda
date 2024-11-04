include(rpcgen)

rpcgen(
  TARGET generated_kafka_data_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/rpc.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/rpc_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)

v_cc_library(
  NAME kd_rpc
  SRCS
    client.cc
    deps.cc
    logger.cc
    serde.cc
    service.cc
  DEPS
    generated_kafka_data_rpc 
    v::cluster
    v::model
    v::rpc
    v::kafka_partition_proxy
    Seastar::seastar
)

# TODO(oren):
# add_subdirectory(tests)
