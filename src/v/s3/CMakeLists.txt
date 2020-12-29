
v_cc_library(
  NAME s3
  SRCS
    signature.cc
    error.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::http
)
