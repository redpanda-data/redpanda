load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "smp_groups",
    srcs = [
        "smp_groups.cc",
    ],
    hdrs = [
        "smp_groups.h",
    ],
    include_prefix = "resource_mgmt",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "storage",
    srcs = [
        "storage.cc",
    ],
    hdrs = [
        "storage.h",
    ],
    include_prefix = "resource_mgmt",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/metrics",
        "//src/v/raft",
        "//src/v/ssx:semaphore",
        "//src/v/storage",
        "//src/v/utils:human",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    include_prefix = "resource_mgmt",
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "available_memory",
    srcs = [
        "available_memory.cc",
    ],
    hdrs = [
        "available_memory.h",
    ],
    include_prefix = "resource_mgmt",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/container:intrusive",
        "//src/v/metrics",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "memory_groups",
    srcs = [
        "memory_groups.cc",
    ],
    hdrs = [
        "memory_groups.h",
    ],
    include_prefix = "resource_mgmt",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "io_priority",
    hdrs = [
        "io_priority.h",
    ],
    include_prefix = "resource_mgmt",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "cpu_scheduling",
    hdrs = [
        "cpu_scheduling.h",
    ],
    include_prefix = "resource_mgmt",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "scheduling_groups_probe",
    hdrs = [
        "scheduling_groups_probe.h",
    ],
    include_prefix = "resource_mgmt",
    visibility = ["//visibility:public"],
    deps = [
        ":cpu_scheduling",
        "//src/v/base",
        "//src/v/config",
        "//src/v/metrics",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "cpu_profiler",
    srcs = [
        "cpu_profiler.cc",
    ],
    hdrs = [
        "cpu_profiler.h",
    ],
    include_prefix = "resource_mgmt",
    visibility = ["//visibility:public"],
    deps = [
        ":logger",
        "//src/v/base",
        "//src/v/config",
        "//src/v/random:generators",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sformat",
        "@abseil-cpp//absl/container:node_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "memory_sampling",
    srcs = [
        "memory_sampling.cc",
    ],
    hdrs = [
        "memory_sampling.h",
    ],
    include_prefix = "resource_mgmt",
    visibility = ["//visibility:public"],
    deps = [
        ":available_memory",
        "//src/v/base",
        "//src/v/config",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sformat",
        "@fmt",
        "@seastar",
    ],
)
