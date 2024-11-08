load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_gtest")

redpanda_cc_btest(
    name = "feature_table_test",
    timeout = "short",
    srcs = [
        "feature_table_test.cc",
    ],
    deps = [
        "//src/v/cluster:features",
        "//src/v/features",
        "//src/v/model",
        "//src/v/security:license",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "enterprise_features_test",
    timeout = "short",
    srcs = [
        "enterprise_features_test.cc",
    ],
    deps = [
        "//src/v/config",
        "//src/v/features:enterprise_features",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
