v_cc_library(
  NAME cloud_io
  SRCS
    io_resources.cc
  DEPS
    Seastar::seastar
    v::config)

