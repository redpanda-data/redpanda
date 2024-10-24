
v_cc_library(
  NAME http
  SRCS
    iobuf_body.cc
    chunk_encoding.cc
    client.cc
    logger.cc
    utils.cc
    request_builder.cc
    rest_client/rest_entity.cc
    utils.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::net
    ada
  DEFINES
    -DBOOST_ASIO_HAS_STD_INVOKE_RESULT
)

add_subdirectory(tests)
