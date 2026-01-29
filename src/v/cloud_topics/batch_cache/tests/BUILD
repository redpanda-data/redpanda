load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "batch_cache_test",
    timeout = "short",
    srcs = [
        "batch_cache_test.cc",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics/batch_cache",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "hydrated_object_index_test",
    timeout = "short",
    srcs = [
        "hydrated_object_index_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/batch_cache:hydrated_object_index",
        "//src/v/model",
        "//src/v/storage:batch_cache",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
