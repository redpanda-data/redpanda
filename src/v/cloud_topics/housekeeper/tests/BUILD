load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "housekeeper_test",
    timeout = "short",
    srcs = [
        "housekeeper_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics/housekeeper",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore:simple",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
