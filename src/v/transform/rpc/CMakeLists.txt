v_cc_library(
  NAME transform_rpc
  HDRS
    serde.h
  SRCS
    serde.cc
  DEPS
    v::cluster
    v::model
    Seastar::seastar
)

