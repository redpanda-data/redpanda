load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "example_proto",
    srcs = ["example.proto"],
    deps = [
        "//proto/redpanda/core/pbgen:options_proto",
        "//proto/redpanda/core/pbgen:rpc_proto",
    ],
)

redpanda_proto_library(
    name = "example_redpanda_proto",
    protos = [":example_proto"],
    visibility = ["//src/v/serde/protobuf/tests:__pkg__"],
)

cc_proto_library(
    name = "example_cc_proto",
    visibility = ["//src/v/serde/protobuf/tests:__pkg__"],
    deps = [":example_proto"],
)
