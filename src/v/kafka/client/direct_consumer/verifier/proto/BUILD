load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "verifier_api_proto",
    srcs = ["verifier.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/pbgen:options_proto",
        "//proto/redpanda/core/pbgen:rpc_proto",
    ],
)

redpanda_proto_library(
    name = "verifier_api",
    protos = [":verifier_api_proto"],
    visibility = ["//visibility:public"],
    deps = [],
)
