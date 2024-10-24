
v_cc_library(
  NAME serde_parquet
  SRCS
    metadata.cc
    encoding.cc
    flattened_schema.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::container
    v::utils
  )
