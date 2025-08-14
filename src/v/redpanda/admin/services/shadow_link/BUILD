load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "shadow_link",
    srcs = ["shadow_link.cc"],
    hdrs = ["shadow_link.h"],
    visibility = ["//src/v/redpanda:__subpackages__"],
    deps = [
        "//proto/redpanda/core/admin:shadow_link_redpanda_proto",
        "//src/v/serde/protobuf:rpc",
    ],
)
