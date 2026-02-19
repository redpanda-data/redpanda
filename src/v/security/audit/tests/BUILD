load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "utils",
    srcs = ["audit_test_utils.cc"],
    hdrs = ["audit_test_utils.h"],
    deps = [
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/random:generators",
        "//src/v/security/audit",
        "//src/v/security/audit:types",
    ],
)

redpanda_cc_gtest(
    name = "audit_test",
    timeout = "short",
    srcs = ["audit_log_test.cc"],
    cpu = 1,
    memory = "512MiB",
    deps = [
        ":utils",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/kafka/client/test:fixture",
        "//src/v/redpanda/tests:fixture",
        "//src/v/security/audit",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
        "@seastar",
    ],
)
