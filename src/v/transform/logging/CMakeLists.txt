v_cc_library(
  NAME transform_logging
  SRCS
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
    v::kd_record_batcher
)

add_subdirectory(tests)
