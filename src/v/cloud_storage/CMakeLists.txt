
v_cc_library(
  NAME cloud_storage
  SRCS
    manifest.cc
    remote.cc
    cache_service.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::http
    v::s3
    v::json
    v::model
    v::cluster
    v::rphashing
)
add_subdirectory(tests)
