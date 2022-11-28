
v_cc_library(
  NAME archival
  SRCS
    archival_policy.cc
    ntp_archiver_service.cc
    probe.cc
    types.cc
    upload_controller.cc
    segment_reupload.cc
    retention_calculator.cc
  DEPS
    Seastar::seastar
    v::bytes
    v::http
    v::cloud_storage_clients
    v::cloud_storage
    v::json
    v::model
    v::storage
    v::cluster
    v::config
    v::rphashing
)
add_subdirectory(tests)
