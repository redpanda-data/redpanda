include(rpcgen)

rpcgen(
  TARGET generated_transform_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/rpc.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/rpc_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)

v_cc_library(
  NAME transform_rpc
  HDRS
    client.h
    logger.h
    serde.h
    service.h
    deps.h
  SRCS
    client.cc
    logger.cc
    serde.cc
    service.cc
    deps.cc
  DEPS
    generated_transform_rpc 
    v::cluster
    v::model
    v::rpc
    v::kafka
    Seastar::seastar
)
