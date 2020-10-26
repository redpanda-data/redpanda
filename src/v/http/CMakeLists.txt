
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
    v::rpc
)

add_subdirectory(demo)
add_subdirectory(tests)
