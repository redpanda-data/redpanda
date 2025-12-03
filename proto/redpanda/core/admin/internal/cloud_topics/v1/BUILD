load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "metastore_proto",
    srcs = ["metastore.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/common/v1:ntp_proto",
        "//proto/redpanda/core/pbgen:options_proto",
        "//proto/redpanda/core/pbgen:rpc_proto",
    ],
)

redpanda_proto_library(
    name = "metastore_redpanda_proto",
    protos = [":metastore_proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/common/v1:ntp_redpanda_proto",
    ],
)

proto_library(
    name = "level_zero_gc_proto",
    srcs = ["level_zero_gc.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/common/v1:ntp_proto",
        "//proto/redpanda/core/pbgen:options_proto",
        "//proto/redpanda/core/pbgen:rpc_proto",
    ],
)

redpanda_proto_library(
    name = "level_zero_gc_redpanda_proto",
    protos = [":level_zero_gc_proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/common/v1:ntp_redpanda_proto",
    ],
)
