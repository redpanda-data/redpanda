load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "fixture",
    hdrs = [
        "fixture.h",
    ],
    implementation_deps = [
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/redpanda:application",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:fixture",
        "//src/v/test_utils:logs",
    ],
)

redpanda_cc_btest(
    name = "cli_parser",
    timeout = "short",
    srcs = [
        "cli_parser_tests.cc",
    ],
    deps = [
        "//src/v/redpanda:cli_parser",
        "@boost//:algorithm",
        "@boost//:program_options",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)
