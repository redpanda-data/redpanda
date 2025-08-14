load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel/pbgen:pbgen.bzl", "redpanda_proto_library")

proto_library(
    name = "admin_proto",
    srcs = ["admin.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/pbgen:options_proto",
        "//proto/redpanda/pbgen:rpc_proto",
    ],
)

proto_library(
    name = "shadow_link_proto",
    srcs = ["shadow_link.proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/common:common_proto",
        "//proto/redpanda/pbgen:options_proto",
        "//proto/redpanda/pbgen:rpc_proto",
        "@googleapis//google/api:field_behavior_proto",
        "@googleapis//google/api:field_info_proto",
        "@googleapis//google/api:resource_proto",
        "@protobuf//:duration_proto",
        "@protobuf//:field_mask_proto",
        "@protobuf//:timestamp_proto",
    ],
)

redpanda_proto_library(
    name = "admin_redpanda_proto",
    protos = [":admin_proto"],
    visibility = ["//visibility:public"],
)

redpanda_proto_library(
    name = "shadow_link_redpanda_proto",
    protos = [":shadow_link_proto"],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/common:common_redpanda_proto",
        "//src/v/serde/protobuf:field_mask",
        "@abseil-cpp//absl/time:time",
    ],
)
