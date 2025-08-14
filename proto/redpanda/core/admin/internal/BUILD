load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "debug_proto",
    srcs = ["debug.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/pbgen:options_proto",
        "//proto/redpanda/pbgen:rpc_proto",
    ],
)

redpanda_proto_library(
    name = "debug_redpanda_proto",
    protos = [":debug_proto"],
    visibility = ["//visibility:public"],
)
