
v_cc_library(
  NAME archival
  SRCS
    archival_policy.cc
    service.cc
    ntp_archiver_service.cc
    probe.cc
    types.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::http
    v::s3
    v::cloud_storage
    v::json
    v::model
    v::storage
    v::cluster
    v::config
    v::rphashing
)
add_subdirectory(tests)
