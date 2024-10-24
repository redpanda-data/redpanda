load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "features",
    srcs = [
        "feature_table.cc",
        "feature_table_snapshot.cc",
        "logger.cc",
    ],
    hdrs = [
        "feature_state.h",
        "feature_table.h",
        "feature_table_snapshot.h",
        "fwd.h",
        "logger.h",
    ],
    implementation_deps = [
        "//src/v/metrics",
    ],
    include_prefix = "features",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cluster:features",
        "//src/v/cluster:version",
        "//src/v/config",
        "//src/v/model",
        "//src/v/security:license",
        "//src/v/serde",
        "//src/v/serde:named_type",
        "//src/v/serde:optional",
        "//src/v/serde:sstring",
        "//src/v/serde:vector",
        "//src/v/storage:record_batch_builder",
        "//src/v/utils:waiter_queue",
        "//src/v/version",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "enterprise_features",
    srcs = [
        "enterprise_features.cc",
    ],
    hdrs = [
        "enterprise_features.h",
    ],
    include_prefix = "features",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:range",
    ],
)
