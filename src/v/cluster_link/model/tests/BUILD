load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "test_model",
    timeout = "short",
    srcs = [
        "test_model.cc",
    ],
    deps = [
        "//src/v/cluster_link/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "test_filters",
    timeout = "short",
    srcs = [
        "test_filters.cc",
    ],
    deps = [
        "//src/v/cluster_link/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
