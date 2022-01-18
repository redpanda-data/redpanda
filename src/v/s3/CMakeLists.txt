
v_cc_library(
  NAME s3
  SRCS
    client.cc
    signature.cc
    error.cc
    client_probe.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::net
  DEFINES
    -DBOOST_ASIO_HAS_STD_INVOKE_RESULT
)
add_subdirectory(tests)
add_subdirectory(test_client)

