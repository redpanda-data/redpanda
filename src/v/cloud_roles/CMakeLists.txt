v_cc_library(
    NAME cloud_roles
    SRCS
    apply_aws_credentials.cc
    apply_credentials.cc
    apply_gcp_credentials.cc
    probe.cc
    refresh_credentials.cc
    request_response_helpers.cc
    types.cc
    DEPS
    Seastar::seastar
    v::http
    v::s3
    v::model
)
