load("//bazel:test.bzl", "redpanda_cc_btest")

redpanda_cc_btest(
    name = "feature_table_test",
    timeout = "short",
    srcs = [
        "feature_table_test.cc",
    ],
    deps = [
        "//src/v/cluster:features",
        "//src/v/features",
        "//src/v/security:license",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)
