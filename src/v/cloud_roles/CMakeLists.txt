v_cc_library(
    NAME cloud_roles
    SRCS
    apply_abs_credentials.cc
    apply_aws_credentials.cc
    apply_credentials.cc
    apply_gcp_credentials.cc
    aws_refresh_impl.cc
    aws_sts_refresh_impl.cc
    gcp_refresh_impl.cc
    probe.cc
    refresh_credentials.cc
    request_response_helpers.cc
    types.cc
    signature.cc
    DEPS
    Seastar::seastar
    v::http
    v::model
    v::utils
)

add_subdirectory(tests)
