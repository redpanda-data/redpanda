load("//bazel:test.bzl", "redpanda_cc_gtest")

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

redpanda_cc_gtest(
    name = "catalog_client_tests",
    timeout = "short",
    srcs = [
        "catalog_client_tests.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/iceberg/rest_client:client",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
