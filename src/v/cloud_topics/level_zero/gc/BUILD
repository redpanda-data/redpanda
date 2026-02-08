load("//bazel:build.bzl", "redpanda_cc_library")
load("//src/v/rpc:compiler.bzl", "redpanda_cc_rpc_library")

package(default_visibility = [
    ":__subpackages__",
    "//src/v/cloud_topics:__pkg__",
])

redpanda_cc_library(
    name = "level_zero_gc_probe",
    srcs = [
        "level_zero_gc_probe.cc",
    ],
    hdrs = [
        "level_zero_gc_probe.h",
    ],
    deps = [
        "//src/v/cloud_topics:types",
        "//src/v/metrics",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "level_zero_gc",
    srcs = [
        "level_zero_gc.cc",
    ],
    hdrs = [
        "level_zero_gc.h",
    ],
    implementation_deps = [
        ":epoch_barrier_coordinator",
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:object_utils",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/ssx:semaphore",
        "//src/v/ssx:work_queue",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":level_zero_gc_probe",
        "//src/v/cloud_io:io_result",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics:types",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "rpc_types",
    hdrs = [
        "rpc_types.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_topics:types",
        "//src/v/rpc",
        "//src/v/serde",
        "//src/v/serde:enum",
    ],
)

redpanda_cc_rpc_library(
    name = "generated_epoch_barrier_rpc",
    src = "rpc.json",
    deps = [
        ":rpc_types",
    ],
)

redpanda_cc_library(
    name = "epoch_barrier_coordinator",
    srcs = [
        "epoch_barrier_coordinator.cc",
    ],
    hdrs = [
        "epoch_barrier_coordinator.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics:logger",
        "//src/v/cluster",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:types",
        "//src/v/rpc",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "epoch_barrier_manager",
    srcs = [
        "epoch_barrier_manager.cc",
    ],
    hdrs = [
        "epoch_barrier_manager.h",
    ],
    implementation_deps = [
        ":epoch_barrier_coordinator",
        ":generated_epoch_barrier_rpc",
        ":level_zero_gc",
        "//src/v/cloud_topics:logger",
        "//src/v/cluster",
        "//src/v/rpc",
        "//src/v/ssx:future_util",
        "//src/v/ssx:semaphore",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":rpc_types",
        "//src/v/base",
        "//src/v/cloud_topics:types",
        "//src/v/model",
        "//src/v/ssx:actor",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "epoch_barrier_service",
    srcs = [
        "service.cc",
    ],
    hdrs = [
        "service.h",
    ],
    implementation_deps = [
        ":epoch_barrier_coordinator",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":generated_epoch_barrier_rpc",
        ":rpc_types",
        "//src/v/base",
        "//src/v/rpc",
        "@seastar",
    ],
)
