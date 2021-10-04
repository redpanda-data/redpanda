
v_cc_library(
  NAME cloud_storage
  SRCS
    cache_service.cc
    manifest.cc
    recursive_directory_walker.cc
    remote.cc
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
