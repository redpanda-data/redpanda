v_cc_library(
  NAME cloud_io
  SRCS
    auth_refresh_bg_op.cc
    io_resources.cc
    io_result.cc
    remote.cc
    transfer_details.cc
  DEPS
    Seastar::seastar
    v::config)

add_subdirectory(tests)
