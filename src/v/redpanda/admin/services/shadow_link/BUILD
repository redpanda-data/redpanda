load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "converter",
    srcs = ["converter.cc"],
    hdrs = ["converter.h"],
    visibility = ["//src/v/redpanda/admin/services/shadow_link:__subpackages__"],
    deps = [
        "//proto/redpanda/core/admin/v2:shadow_link_redpanda_proto",
        "//src/v/cluster_link/model",
    ],
)

redpanda_cc_library(
    name = "shadow_link",
    srcs = ["shadow_link.cc"],
    hdrs = ["shadow_link.h"],
    implementation_deps = [
        ":converter",
        "//src/v/cluster_link",
    ],
    visibility = ["//src/v/redpanda:__subpackages__"],
    deps = [
        "//proto/redpanda/core/admin/v2:shadow_link_redpanda_proto",
        "//src/v/cluster",
        "//src/v/cluster_link:fwd",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "@seastar",
    ],
)
