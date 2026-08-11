load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "converter",
    srcs = ["converter.cc"],
    hdrs = ["converter.h"],
    implementation_deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/config",
        "//src/v/crypto",
        "//src/v/serde/protobuf:rpc",
        "//src/v/utils:base64",
        "@seastar",
    ],
    visibility = ["//src/v/redpanda/admin/services/shadow_link:__subpackages__"],
    deps = [
        "//proto/redpanda/core/admin/v2:shadow_link_redpanda_proto",
        "//src/v/cluster_link/model",
    ],
)

redpanda_cc_library(
    name = "err",
    srcs = ["err.cc"],
    hdrs = ["err.h"],
    implementation_deps = [
        "//src/v/serde/protobuf:rpc",
    ],
    visibility = ["//src/v/redpanda:__subpackages__"],
    deps = [
        "//src/v/base",
        "//src/v/cluster_link:errc",
        "//src/v/ssx:sformat",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "shadow_link",
    srcs = ["shadow_link.cc"],
    hdrs = ["shadow_link.h"],
    implementation_deps = [
        ":converter",
        ":err",
        "//src/v/cluster_link",
        "//src/v/redpanda/admin/services:utils",
    ],
    visibility = ["//src/v/redpanda:__subpackages__"],
    deps = [
        "//proto/redpanda/core/admin/v2:shadow_link_redpanda_proto",
        "//src/v/cluster",
        "//src/v/cluster_link:fwd",
        "//src/v/cluster_link/model",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "@seastar",
    ],
)
