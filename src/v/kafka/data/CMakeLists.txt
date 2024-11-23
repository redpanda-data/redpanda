v_cc_library(
  NAME kd_logger
  HDRS
    logger.h
  SRCS
    logger.cc
  DEPS
    v::base
)

v_cc_library(
  NAME kafka_partition_proxy
  SRCS
    partition_proxy.cc
    replicated_partition.cc
  DEPS
    Seastar::seastar
    v::cloud_storage
    v::cluster
    v::kafka_protocol
    v::kd_logger
    v::model
)

v_cc_library(
  NAME kd_record_batcher
  HDRS
    record_batcher.h
  SRCS
    record_batcher.cc
  DEPS
    v::kd_logger
    v::model
    v::storage
)

add_subdirectory(test)
