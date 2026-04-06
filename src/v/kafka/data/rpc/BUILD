load("//bazel:build.bzl", "redpanda_cc_library")
load("//src/v/rpc:compiler.bzl", "redpanda_cc_rpc_library")

redpanda_cc_rpc_library(
    name = "kafka_data_rpc",
    src = "rpc.json",
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
        "fwd.h",
        "serde.h",
        "service.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":kafka_data_rpc",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/data:log_reader_config",
        "//src/v/kafka/data:partition_proxy",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/rpc",
        "//src/v/serde",
        "//src/v/serde:map",
        "//src/v/serde:vector",
        "//src/v/ssx:async_algorithm",
        "//src/v/ssx:semaphore",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/transform/stm",
        "//src/v/utils:backoff_policy",
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
