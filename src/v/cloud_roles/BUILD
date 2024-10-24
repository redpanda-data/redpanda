load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_roles:__subpackages__"])

redpanda_cc_library(
    name = "logger",
    hdrs = [
        "logger.h",
    ],
    include_prefix = "cloud_roles",
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "types",
    srcs = [
        "types.cc",
    ],
    hdrs = [
        "types.h",
    ],
    include_prefix = "cloud_roles",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/utils:named_type",
        "@boost//:beast",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "request_response_helpers",
    srcs = [
        "request_response_helpers.cc",
    ],
    hdrs = [
        "request_response_helpers.h",
    ],
    include_prefix = "cloud_roles",
    deps = [
        ":logger",
        ":types",
        "//src/v/bytes:iostream",
        "//src/v/bytes:streambuf",
        "//src/v/config",
        "//src/v/http",
        "//src/v/json",
        "@rapidjson",
    ],
)

redpanda_cc_library(
    name = "signature",
    srcs = [
        "signature.cc",
    ],
    hdrs = [
        "signature.h",
    ],
    include_prefix = "cloud_roles",
    deps = [
        ":logger",
        ":request_response_helpers",
        ":types",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/config",
        "//src/v/hashing:secure",
        "//src/v/http",
        "//src/v/http:utils",
        "//src/v/ssx:sformat",
        "//src/v/utils:base64",
        "//src/v/utils:named_type",
        "@abseil-cpp//absl/strings",
        "@boost//:algorithm",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "cloud_roles",
    srcs = [
        "apply_abs_credentials.cc",
        "apply_abs_credentials.h",
        "apply_abs_oauth_credentials.cc",
        "apply_abs_oauth_credentials.h",
        "apply_aws_credentials.cc",
        "apply_aws_credentials.h",
        "apply_credentials.cc",
        "apply_gcp_credentials.cc",
        "apply_gcp_credentials.h",
        "aws_refresh_impl.cc",
        "aws_refresh_impl.h",
        "aws_sts_refresh_impl.cc",
        "aws_sts_refresh_impl.h",
        "azure_aks_refresh_impl.cc",
        "azure_aks_refresh_impl.h",
        "azure_vm_refresh_impl.cc",
        "azure_vm_refresh_impl.h",
        "gcp_refresh_impl.cc",
        "gcp_refresh_impl.h",
        "probe.cc",
        "refresh_credentials.cc",
    ],
    hdrs = [
        "apply_credentials.h",
        "probe.h",
        "refresh_credentials.h",
    ],
    include_prefix = "cloud_roles",
    visibility = ["//visibility:public"],
    deps = [
        ":logger",
        ":request_response_helpers",
        ":signature",
        ":types",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/bytes:streambuf",
        "//src/v/config",
        "//src/v/hashing:secure",
        "//src/v/http",
        "//src/v/http:utils",
        "//src/v/json",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/net",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sformat",
        "//src/v/thirdparty/ada",
        "//src/v/utils:base64",
        "//src/v/utils:file_io",
        "//src/v/utils:named_type",
        "@abseil-cpp//absl/strings",
        "@boost//:algorithm",
        "@boost//:property_tree",
        "@fmt",
        "@rapidjson",
        "@seastar",
    ],
)
