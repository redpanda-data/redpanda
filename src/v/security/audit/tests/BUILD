load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "audit_test",
    timeout = "short",
    srcs = ["audit_log_test.cc"],
    cpu = 1,
    memory = "256MiB",
    deps = [
        "//src/v/cluster",
        "//src/v/kafka/client/test:fixture",
        "//src/v/redpanda/tests:fixture",
        "//src/v/security/audit",
        "//src/v/security/audit:types",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
    ],
)
