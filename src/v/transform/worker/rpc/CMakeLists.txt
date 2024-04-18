include(rpcgen)

rpcgen(
  TARGET generated_transform_worker_rpc
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/rpc.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/rpc_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
)

v_cc_library(
  NAME transform_worker_rpc
  SRCS
    types.cc
    errc.cc
  DEPS
    generated_transform_worker_rpc 
    v::model
    v::serde
)
