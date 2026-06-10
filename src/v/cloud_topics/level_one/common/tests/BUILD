load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "object_test",
    timeout = "short",
    srcs = [
        "object_test.cc",
    ],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "object_utils_test",
    timeout = "short",
    srcs = [
        "object_utils_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/common:object_utils",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "single_flight_test",
    timeout = "short",
    srcs = [
        "single_flight_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:single_flight",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
