
v_cc_library(
  NAME archival
  SRCS
    archival_policy.cc
    service.cc
    ntp_archiver_service.cc
    manifest.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::http
    v::s3
    v::json
    v::model
    v::storage
    v::cluster
    v::config
    v::rphashing
)
add_subdirectory(tests)
