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
    name = "level_zero_gc_types",
    hdrs = [
        "level_zero_gc_types.h",
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
    name = "level_zero_gc",
    srcs = [
        "level_zero_gc.cc",
    ],
    hdrs = [
        "level_zero_gc.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:object_utils",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/random:time_jitter",
        "//src/v/ssx:semaphore",
        "//src/v/ssx:work_queue",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":level_zero_gc_probe",
        ":level_zero_gc_types",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics:types",
        "//src/v/random:generators",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "rpc_types",
    hdrs = [
        "rpc_types.h",
    ],
    deps = [
        "//src/v/cloud_topics:types",
        "//src/v/serde",
        "//src/v/serde:enum",
        "//src/v/serde:optional",
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
    name = "epoch_barrier_service",
    srcs = [
        "service.cc",
    ],
    hdrs = [
        "service.h",
    ],
    implementation_deps = [
        ":epoch_barrier",
    ],
    visibility = [
        "//src/v/redpanda:__pkg__",
    ],
    deps = [
        ":generated_epoch_barrier_rpc",
        ":rpc_types",
        "//src/v/base",
        "//src/v/rpc",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "epoch_barrier",
    srcs = [
        "epoch_barrier.cc",
    ],
    hdrs = [
        "epoch_barrier.h",
    ],
    implementation_deps = [
        ":generated_epoch_barrier_rpc",
        "//src/v/cloud_topics:inflight_write_tracker",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm_api",
        "//src/v/cluster",
        "//src/v/rpc",
        "//src/v/ssx:future_util",
        "//src/v/ssx:when_all",
    ],
    deps = [
        ":rpc_types",
        "//src/v/base",
        "//src/v/cloud_topics:types",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/rpc",
        "@seastar",
    ],
)
