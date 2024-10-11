v_cc_library(
  NAME cloud_io
  SRCS
    auth_refresh_bg_op.cc
    basic_cache_service_api.cc
    io_resources.cc
    io_result.cc
    remote.cc
    transfer_details.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::cloud_roles
    v::cloud_storage_clients
    v::config
    v::utils)

add_subdirectory(tests)
