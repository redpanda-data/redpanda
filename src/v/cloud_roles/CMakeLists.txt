v_cc_library(
    NAME cloud_roles
    SRCS
    apply_credentials.cc
    types.cc
    DEPS
    Seastar::seastar
    v::http
    v::s3
    v::model
)
