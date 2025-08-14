load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_btest_no_seastar", "redpanda_cc_gtest")

redpanda_cc_btest_no_seastar(
    name = "outcome_test",
    timeout = "short",
    srcs = [
        "outcome_test.cc",
    ],
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_btest(
    name = "outcome_utils_test",
    timeout = "short",
    srcs = [
        "outcome_utils_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/test_utils:seastar_boost",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "assert_log_holder_test",
    timeout = "short",
    srcs = [
        "assert_log_holder_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "format_to_test",
    timeout = "short",
    srcs = [
        "format_to_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
