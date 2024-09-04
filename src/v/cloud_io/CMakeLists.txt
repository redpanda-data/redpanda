v_cc_library(
  NAME cloud_io
  SRCS
    auth_refresh_bg_op.cc
    io_resources.cc
    transfer_details.cc
  DEPS
    Seastar::seastar
    v::config)

