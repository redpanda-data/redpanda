
v_cc_library(
  NAME serde_parquet
  SRCS
    metadata.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::container
    v::utils
  )
