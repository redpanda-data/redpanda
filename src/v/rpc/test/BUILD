load("//bazel:test.bzl", "redpanda_cc_btest")
load("//src/v/rpc:compiler.bzl", "redpanda_cc_rpc_library")

redpanda_cc_btest(
    name = "netbuf_test",
    timeout = "short",
    srcs = [
        "netbuf_tests.cc",
        "test_types.h",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/rpc",
        "//src/v/test_utils:seastar_boost",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "roundtrip_test",
    timeout = "short",
    srcs = [
        "roundtrip_tests.cc",
        "test_types.h",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/reflection:adl",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "response_handler_test",
    timeout = "short",
    srcs = [
        "response_handler_tests.cc",
    ],
    deps = [
        "//src/v/rpc",
        "//src/v/ssx:semaphore",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "serialization_test",
    timeout = "short",
    srcs = [
        "serialization_test.cc",
        "test_types.h",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/reflection:adl",
        "//src/v/reflection:arity",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_rpc_library(
    name = "cycling_rpc",
    src = "cycling_service.json",
)

redpanda_cc_rpc_library(
    name = "echo_rpc",
    src = "echo_service.json",
)

redpanda_cc_rpc_library(
    name = "echo_v2_rpc",
    src = "echo_v2_service.json",
)
