
v_cc_library(
  NAME http
  SRCS
    logger.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::rpc
)

