load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "metrics",
    srcs = [
        "host_metrics_watcher.cc",
        "metrics.cc",
        "metrics_registry.cc",
    ],
    hdrs = [
        "aggregate_metrics_watcher.h",
        "host_metrics_watcher.h",
        "metrics.h",
        "metrics_registry.h",
        "prometheus_sanitize.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/ssx:future_util",
        "//src/v/ssx:sformat",
        "//src/v/utils:device_utils",
        "//src/v/utils:file_io",
        "//src/v/utils:hdr_hist",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)
