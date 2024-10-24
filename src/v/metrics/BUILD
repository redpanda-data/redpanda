load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "metrics",
    srcs = [
        "metrics.cc",
        "metrics_registry.cc",
    ],
    hdrs = [
        "aggregate_metrics_watcher.h",
        "metrics.h",
        "metrics_registry.h",
        "prometheus_sanitize.h",
    ],
    include_prefix = "metrics",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/ssx:sformat",
        "//src/v/utils:hdr_hist",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)
