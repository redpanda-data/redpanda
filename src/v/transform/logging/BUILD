load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    include_prefix = "transform/logging",
    visibility = ["//src/v/transform:__subpackages__"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "logging",
    srcs = [
        "event.cc",
        "event.h",
        "log_manager.cc",
        "probes.cc",
        "probes.h",
        "record_batcher.cc",
        "record_batcher.h",
        "rpc_client.cc",
    ],
    hdrs = [
        "errc.h",
        "fwd.h",
        "io.h",
        "log_manager.h",
        "rpc_client.h",
    ],
    include_prefix = "transform/logging",
    visibility = ["//visibility:public"],
    deps = [
        ":logger",
        "//src/v/base",
        "//src/v/bytes:streambuf",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/hashing:murmur",
        "//src/v/json",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/random:time_jitter",
        "//src/v/ssx:semaphore",
        "//src/v/storage:record_batch_builder",
        "//src/v/strings:utf8",
        "//src/v/transform/rpc",
        "//src/v/utils:absl_sstring_hash",
        "//src/v/utils:human",
        "//src/v/utils:named_type",
        "@abseil-cpp//absl/algorithm:container",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/strings",
        "@seastar",
    ],
)
