load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@protobuf//bazel:proto_library.bzl", "proto_library")
load("@rules_go//proto:def.bzl", "go_proto_library")

proto_library(
    name = "options_proto",
    srcs = ["options.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "@protobuf//:descriptor_proto",
    ],
)

go_proto_library(
    name = "options_go_proto",
    importpath = "github.com/redpanda-data/redpanda/proto/redpanda/core/pbgen/options",
    protos = [":options_proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "options_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":options_proto"],
)

proto_library(
    name = "rpc_proto",
    srcs = ["rpc.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "@protobuf//:descriptor_proto",
    ],
)

go_proto_library(
    name = "rpc_go_proto",
    importpath = "github.com/redpanda-data/redpanda/proto/redpanda/core/pbgen/rpc",
    protos = [":rpc_proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "rpc_cc_proto",
    visibility = ["//visibility:public"],
    deps = [
        ":rpc_proto",
    ],
)
