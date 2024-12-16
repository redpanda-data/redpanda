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

redpanda_cc_btest(
    name = "retention_property_test",
    timeout = "short",
    srcs = [
        "retention_property_test.cc",
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
    name = "config_store_test",
    timeout = "short",
    srcs = [
        "config_store_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/json",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "socket_address_convert_test",
    timeout = "short",
    srcs = [
        "socket_address_convert_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
        "@yaml-cpp",
    ],
)

redpanda_cc_btest(
    name = "tls_config_convert_test",
    timeout = "short",
    srcs = [
        "tls_config_convert_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/config",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:to_string",
        "@seastar",
        "@seastar//:testing",
        "@yaml-cpp",
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
