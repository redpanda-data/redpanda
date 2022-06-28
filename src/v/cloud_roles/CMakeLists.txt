v_cc_library(
    NAME cloud_roles
    SRCS
    apply_aws_credentials.cc
    apply_credentials.cc
    apply_gcp_credentials.cc
    probe.cc
    types.cc
    signature.cc
    DEPS
    Seastar::seastar
    v::http
    v::model
)

add_subdirectory(tests)
