v_cc_library(
  NAME transform_rpc
  HDRS
    logger.h
    serde.h
  SRCS
    serde.cc
    logger.cc
  DEPS
    v::cluster
    v::model
    Seastar::seastar
)

