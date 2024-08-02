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
    include_prefix = "security",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/json",
        "//src/v/ssx:sformat",
        "//src/v/thirdparty/ada",
        "@boost//:algorithm",
        "@fmt",
        "@re2",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "license",
    srcs = [
        "license.cc",
    ],
    hdrs = [
        "license.h",
    ],
    include_prefix = "security",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/crypto",
        "//src/v/hashing:secure",
        "//src/v/json",
        "//src/v/serde",
        "//src/v/serde:chrono",
        "//src/v/serde:enum",
        "//src/v/serde:sstring",
        "//src/v/utils:base64",
        "@fmt",
        "@seastar",
    ],
)
