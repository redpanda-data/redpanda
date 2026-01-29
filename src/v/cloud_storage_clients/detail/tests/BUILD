load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "registry_test",
    timeout = "short",
    srcs = [
        "registry_test.cc",
    ],
    cpu = 2,
    deps = [
        "//src/v/base",
        "//src/v/cloud_storage_clients/detail:registry",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
