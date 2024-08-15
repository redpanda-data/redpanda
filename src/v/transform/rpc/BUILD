load("//bazel:build.bzl", "redpanda_cc_library")
load("//src/v/rpc:compiler.bzl", "redpanda_cc_rpc_library")

redpanda_cc_rpc_library(
    name = "transform_rpc",
    src = "rpc.json",
    include_prefix = "transform/rpc",
)

redpanda_cc_library(
    name = "rpc",
    srcs = [
        "client.cc",
        "deps.cc",
        "logger.cc",
        "logger.h",
        "serde.cc",
        "service.cc",
    ],
    hdrs = [
        "client.h",
        "deps.h",
        "serde.h",
        "service.h",
    ],
    include_prefix = "transform/rpc",
    visibility = ["//visibility:public"],
    deps = [
        ":transform_rpc",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:fragmented_vector",
        "//src/v/kafka/server",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/resource_mgmt:io_priority",
        "//src/v/rpc",
        "//src/v/serde",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/transform/stm",
        "//src/v/utils:to_string",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:fusion",
        "@boost//:outcome",
        "@boost//:range",
        "@fmt",
        "@seastar",
    ],
)
