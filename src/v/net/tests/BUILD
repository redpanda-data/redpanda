load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_gtest")

cc_binary(
    name = "uds_sustained_bench",
    testonly = True,
    srcs = ["uds_sustained_bench.cc"],
    deps = [
        ":uds_bench_common",
        "//src/v/base",
        "@boost//:program_options",
        "@fmt",
        "@seastar",
    ],
)

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

redpanda_cc_btest(
    name = "uds_listener_test",
    timeout = "short",
    srcs = [
        "uds_listener_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/net:uds_path",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

# Shared fixture for the four UDS bench files. Header-only, so it
# compiles into each bench's TU — the benches differ in what they time,
# not in how they set up the socket pair. redpanda_cc_library sets
# include_prefix = "net/tests" from the package path, so consumers
# write `#include "net/tests/uds_bench_common.hh"`.
redpanda_cc_library(
    name = "uds_bench_common",
    hdrs = ["uds_bench_common.hh"],
    visibility = ["//visibility:private"],
    deps = [
        "//src/v/base",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "uds_rtt_bench_rpbench",
    srcs = [
        "uds_rtt_bench.cc",
    ],
    deps = [
        ":uds_bench_common",
        "//src/v/base",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "uds_throughput_bench_rpbench",
    srcs = [
        "uds_throughput_bench.cc",
    ],
    deps = [
        ":uds_bench_common",
        "//src/v/base",
        "@fmt",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "uds_connect_bench_rpbench",
    srcs = [
        "uds_connect_bench.cc",
    ],
    deps = [
        ":uds_bench_common",
        "//src/v/base",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "uds_concurrency_bench_rpbench",
    srcs = [
        "uds_concurrency_bench.cc",
    ],
    deps = [
        ":uds_bench_common",
        "//src/v/base",
        "@seastar",
        "@seastar//:benchmark",
    ],
)
