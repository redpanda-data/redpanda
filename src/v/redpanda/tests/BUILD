load("//bazel:test.bzl", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "fixture_base",
    implementation_deps = [
    ],
    include_prefix = "redpanda/tests",
    visibility = ["//visibility:private"],
    deps = [
        "//src/v/redpanda:application",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:fixture",
    ],
)

redpanda_test_cc_library(
    name = "fixture_btest",
    hdrs = [
        "fixture.h",
    ],
    implementation_deps = [
    ],
    include_prefix = "redpanda/tests",
    visibility = ["//visibility:public"],
    deps = [
        ":fixture_base",
        "//src/v/cloud_storage/tests:s3_imposter_btest",
    ],
)

redpanda_test_cc_library(
    name = "fixture_gtest",
    hdrs = [
        "fixture.h",
    ],
    implementation_deps = [
    ],
    include_prefix = "redpanda/tests",
    visibility = ["//visibility:public"],
    deps = [
        ":fixture_base",
        "//src/v/cloud_storage/tests:s3_imposter_gtest",
    ],
)
