load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = ["//src/v/transform/logging:__subpackages__"],
)

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    include_prefix = "transform/logging",
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "probes",
    srcs = [
        "probes.cc",
    ],
    hdrs = [
        "probes.h",
    ],
    implementation_deps = [
        "//src/v/config",
    ],
    include_prefix = "transform/logging",
    deps = [
        "//src/v/base",
        "//src/v/metrics",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "record_batcher",
    srcs = ["record_batcher.cc"],
    hdrs = ["record_batcher.h"],
    implementation_deps = [
        ":logger",
        "//src/v/storage:record_batch_builder",
        "//src/v/utils:human",
        "@abseil-cpp//absl/algorithm:container",
    ],
    include_prefix = "transform/logging",
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "event",
    srcs = ["event.cc"],
    hdrs = ["event.h"],
    implementation_deps = [
        "//src/v/json",
    ],
    include_prefix = "transform/logging",
    deps = [
        "//src/v/base",
        "//src/v/bytes:streambuf",
        "//src/v/model",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "logging",
    srcs = [
        "log_manager.cc",
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
        ":event",
        ":logger",
        ":probes",
        ":record_batcher",
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
