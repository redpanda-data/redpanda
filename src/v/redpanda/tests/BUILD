load("//bazel:test.bzl", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "fixture",
    hdrs = [
        "fixture.h",
    ],
    implementation_deps = [
    ],
    include_prefix = "redpanda/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_storage/tests:s3_imposter",
        "//src/v/redpanda:application",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:seastar_boost",
    ],
)
