load("//bazel:test.bzl", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "manual_mixin",
    hdrs = [
        "manual_mixin.h",
    ],
    implementation_deps = [
    ],
    include_prefix = "cluster/cloud_metadata/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/test_utils:scoped_config",
    ],
)
