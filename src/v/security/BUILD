load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "config",
    srcs = [
        "config_bsl.cc",
        "config_rcl.cc",
        "gssapi_rule.cc",
        "logger.cc",
        "mtls_rule.cc",
    ],
    hdrs = [
        "config.h",
        "gssapi_rule.h",
        "logger.h",
        "mtls_rule.h",
        "oidc_error.h",
        "oidc_principal_mapping.h",
        "oidc_url_parser.h",
    ],
    exclude_layering_check = True,
    include_prefix = "security",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/json",
        "//src/v/ssx:sformat",
        "@ada",
        "@boost//:algorithm",
        "@fmt",
        "@seastar",
    ],
)
