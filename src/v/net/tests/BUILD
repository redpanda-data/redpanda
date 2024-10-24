load("//bazel:test.bzl", "redpanda_cc_btest")

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
