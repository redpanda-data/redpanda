load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "common_proto",
    srcs = ["acl.proto"],
    visibility = ["//visibility:public"],
    deps = ["//proto/redpanda/core/pbgen:options_proto"],
)

redpanda_proto_library(
    name = "common_redpanda_proto",
    protos = [":common_proto"],
    visibility = ["//visibility:public"],
)
