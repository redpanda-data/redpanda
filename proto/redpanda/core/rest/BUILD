load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "iceberg_proto",
    srcs = [
        "iceberg.proto",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/pbgen:options_proto",
    ],
)

redpanda_proto_library(
    name = "iceberg_redpanda_proto",
    protos = [
        ":iceberg_proto",
    ],
    visibility = ["//visibility:public"],
    deps = [],
)
