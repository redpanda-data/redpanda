load("//bazel:build.bzl", "redpanda_cc_library")

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
