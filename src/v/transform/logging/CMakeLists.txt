v_cc_library(
  NAME transform_logging
  HDRS
    record_batcher.h
    event.h
    io.h
    log_manager.h
    logger.h
    rpc_client.h
  SRCS
    record_batcher.cc
    event.cc
    log_manager.cc
    logger.cc
    rpc_client.cc
  DEPS
    v::config
    v::cluster
    v::model
    v::transform_rpc
)

add_subdirectory(tests)
