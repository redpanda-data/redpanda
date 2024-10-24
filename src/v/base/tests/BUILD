load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_btest_no_seastar")

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
