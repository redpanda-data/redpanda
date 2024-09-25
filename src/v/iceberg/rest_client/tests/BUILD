load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "rest_client_parser_tests",
    timeout = "short",
    srcs = [
        "parser_tests.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/iceberg/rest_client:rest_client_parsers",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "retry_policy_tests",
    timeout = "short",
    srcs = [
        "retry_policy_tests.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/iceberg/rest_client:retry_policy",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
