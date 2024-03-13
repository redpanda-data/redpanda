
v_cc_library(
  NAME http
  SRCS
    iobuf_body.cc
    chunk_encoding.cc
    client.cc
    logger.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::net
  DEFINES
    -DBOOST_ASIO_HAS_STD_INVOKE_RESULT
)

add_subdirectory(tests)
