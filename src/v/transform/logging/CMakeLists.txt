v_cc_library(
  NAME transform_logging
  SRCS
    record_batcher.cc
    event.cc
    log_manager.cc
    logger.cc
    rpc_client.cc
    probes.cc
  DEPS
    v::config
    v::cluster
    v::model
    v::transform_rpc
)

add_subdirectory(tests)
