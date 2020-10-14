
v_cc_library(
  NAME http
  SRCS
    chunk_encoding.cc
    logger.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::rpc
)

