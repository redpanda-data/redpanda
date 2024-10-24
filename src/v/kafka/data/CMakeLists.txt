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
