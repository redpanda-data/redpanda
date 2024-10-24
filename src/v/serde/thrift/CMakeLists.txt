v_cc_library(
  NAME serde_thrift
  SRCS
    compact.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::utils
  )
