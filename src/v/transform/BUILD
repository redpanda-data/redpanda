load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    include_prefix = "transform",
    visibility = ["//src/v/transform:__subpackages__"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "commit_batcher",
    srcs = [
        "commit_batcher.cc",
    ],
    hdrs = [
        "commit_batcher.h",
    ],
    include_prefix = "transform",
    visibility = ["//src/v/transform:__subpackages__"],
    deps = [
        ":logger",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/model",
        "//src/v/rpc",
        "//src/v/ssx:semaphore",
        "//src/v/ssx:sleep_abortable",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "transfer_queue",
    hdrs = [
        "transfer_queue.h",
    ],
    include_prefix = "transform",
    visibility = ["//src/v/transform:__subpackages__"],
    deps = [
        ":memory_limiter",
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "memory_limiter",
    srcs = [
        "memory_limiter.cc",
    ],
    hdrs = [
        "memory_limiter.h",
    ],
    include_prefix = "transform",
    visibility = ["//src/v/transform:__subpackages__"],
    deps = [
        "//src/v/base",
        "//src/v/utils:human",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "fwd",
    hdrs = [
        "fwd.h",
    ],
    include_prefix = "transform",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "impl",
    srcs = [
        "probe.cc",
        "transform_manager.cc",
        "transform_processor.cc",
    ],
    hdrs = [
        "io.h",
        "probe.h",
        "transform_logger.h",
        "transform_manager.h",
        "transform_processor.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/metrics",
        "//src/v/random:time_jitter",
        "//src/v/rpc",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sformat",
        "//src/v/storage:parser_utils",
        "//src/v/utils:human",
        "@abseil-cpp//absl/algorithm:container",
    ],
    include_prefix = "transform",
    visibility = ["//src/v/transform:__subpackages__"],
    deps = [
        ":commit_batcher",
        ":fwd",
        ":memory_limiter",
        ":transfer_queue",
        "//src/v/base",
        "//src/v/model",
        "//src/v/ssx:work_queue",
        "//src/v/transform/logging",
        "//src/v/utils:prefix_logger",
        "//src/v/wasm:api",
        "//src/v/wasm:cache",
        "//src/v/wasm:transform_probe",
        "//src/v/wasm:wasi_logger",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "transform",
    srcs = [
        "api.cc",
    ],
    hdrs = [
        "api.h",
    ],
    implementation_deps = [
        ":commit_batcher",
        ":impl",
        ":logger",
        "//src/v/config",
        "//src/v/kafka/data:partition_proxy",
        "//src/v/kafka/utils:txn_reader",
        "//src/v/resource_mgmt:io_priority",
        "//src/v/rpc",
        "//src/v/ssx:future_util",
        "//src/v/ssx:semaphore",
        "//src/v/transform/rpc",
        "//src/v/wasm:cache",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:range",
    ],
    include_prefix = "transform",
    visibility = ["//visibility:public"],
    deps = [
        ":fwd",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/features",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/storage",
        "//src/v/transform/logging",
        "//src/v/wasm:api",
        "@boost//:outcome",
        "@fmt",
        "@seastar",
    ],
)
