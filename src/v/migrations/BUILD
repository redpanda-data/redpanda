load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "migrations",
    srcs = [
        "cloud_storage_config.cc",
        "feature_migrator.cc",
        "rbac_migrator.cc",
        "shard_placement_migrator.cc",
    ],
    hdrs = [
        "cloud_storage_config.h",
        "feature_migrator.h",
        "migrators.h",
        "rbac_migrator.h",
        "shard_placement_migrator.h",
    ],
    include_prefix = "migrations",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/features",
        "//src/v/security",
        "//src/v/ssx:future_util",
        "@seastar",
    ],
)
