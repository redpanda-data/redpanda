load("//bazel:test.bzl", "redpanda_cc_btest_no_seastar")

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
