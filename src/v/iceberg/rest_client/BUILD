load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/iceberg:__subpackages__"])

redpanda_cc_library(
    name = "rest_client_types",
    hdrs = [
        "types.h",
    ],
    include_prefix = "iceberg/rest_client",
    deps = [
        "//src/v/base",
        "//src/v/thirdparty/ada",
        "@boost//:beast",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "rest_client_parsers",
    srcs = [
        "parsers.cc",
    ],
    hdrs = [
        "parsers.h",
    ],
    include_prefix = "iceberg/rest_client",
    deps = [
        ":rest_client_types",
        "//src/v/json",
        "//src/v/utils:named_type",
    ],
)

redpanda_cc_library(
    name = "retry_policy",
    srcs = [
        "retry_policy.cc",
    ],
    hdrs = [
        "retry_policy.h",
    ],
    include_prefix = "iceberg/rest_client",
    deps = [
        ":rest_client_types",
        "//src/v/http",
        "//src/v/net",
    ],
)
