
v_cc_library(
  NAME serde_parquet
  SRCS
    metadata.cc
    encoding.cc
    flattened_schema.cc
    value.cc
    schema.cc
    shredder.cc
    column_writer.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::container
    v::utils
    v::serde_thrift
  )
