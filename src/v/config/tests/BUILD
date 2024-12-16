load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_gtest")

redpanda_cc_btest(
    name = "bounded_property_test",
    timeout = "short",
    srcs = [
        "bounded_property_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "enum_property_test",
    timeout = "short",
    srcs = [
        "enum_property_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "enterprise_property_test",
    timeout = "short",
    srcs = [
        "enterprise_property_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
