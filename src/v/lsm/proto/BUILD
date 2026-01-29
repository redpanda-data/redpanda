load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "manifest_proto",
    srcs = ["manifest.proto"],
    deps = ["//proto/redpanda/core/pbgen:options_proto"],
)

redpanda_proto_library(
    name = "manifest_redpanda_proto",
    protos = [":manifest_proto"],
    visibility = ["//visibility:public"],
)
