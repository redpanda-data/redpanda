load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_cc_btest(
    name = "connection_rate_test",
    timeout = "short",
    srcs = [
        "connection_rate_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/net",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "conn_quota_test",
    timeout = "short",
    srcs = [
        "conn_quota_test.cc",
    ],
    cpu = 4,
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/net",
        "//src/v/test_utils:seastar_boost",
        "@boost//:range",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "dns_test",
    timeout = "short",
    srcs = [
        "dns.cc",
    ],
    deps = [
        "//src/v/net",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:unresolved_address",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "connection_test",
    timeout = "short",
    srcs = [
        "connection.cc",
    ],
    deps = [
        "//src/v/net",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "connect_authority_test",
    timeout = "short",
    srcs = [
        "connect_authority_test.cc",
    ],
    deps = [
        "//src/v/net",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_test_cc_library(
    name = "dns_test_utils",
    hdrs = [
        "dns_test_utils.h",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "dns_liveness_test",
    timeout = "moderate",
    srcs = [
        "dns_liveness_test.cc",
    ],
    deps = [
        ":dns_test_utils",
        "//src/v/base",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "dns_recovery_test",
    timeout = "short",
    srcs = [
        "dns_recovery_test.cc",
    ],
    deps = [
        ":dns_test_utils",
        "//src/v/base",
        "//src/v/net",
        "//src/v/test_utils:gtest",
        "//src/v/utils:unresolved_address",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "dial_test",
    timeout = "short",
    srcs = [
        "dial_test.cc",
    ],
    deps = [
        "//src/v/net",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "connect_response_parser_test",
    timeout = "short",
    srcs = [
        "connect_response_parser_test.cc",
    ],
    deps = [
        "//src/v/net",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
