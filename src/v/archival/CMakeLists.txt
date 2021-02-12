
v_cc_library(
  NAME archival
  SRCS
    archival_policy.cc
    ntp_archiver_service.cc
    manifest.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::http
    v::s3
)
add_subdirectory(tests)
