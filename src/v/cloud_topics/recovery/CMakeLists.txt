v_cc_library(
  NAME cloud_topics_recovery
  SRCS
    snapshot_io.cc
    snapshot_manager.cc
    managed_partition.cc
    logger.cc
  DEPS
    v::cloud_topics_dl_stm
    v::cluster
)
