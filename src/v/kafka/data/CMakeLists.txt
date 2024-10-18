v_cc_library(
  NAME kafka_partition_proxy
  SRCS
    logger.cc
    partition_proxy.cc
    replicated_partition.cc
  DEPS
    Seastar::seastar
    v::cloud_storage
    v::cluster
    v::kafka_protocol
    v::model
)

v_cc_library(
  NAME kd_record_batcher
  HDRS
    record_batcher.h
  SRCS
    record_batcher.cc
  DEPS
    v::model
    v::storage
)
