load("@rules_python//python:defs.bzl", "py_binary")
load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_cc_btest")
load("//src/v/rpc:compiler.bzl", "redpanda_cc_rpc_library")

py_binary(
    name = "compiler",
    srcs = ["rpc_compiler.py"],
    main = "rpc_compiler.py",
    visibility = ["//visibility:public"],
    deps = [
        "@python_deps//jinja2",
    ],
)

redpanda_cc_library(
    name = "rpc",
    srcs = [
        "connection_cache.cc",
        "connection_set.cc",
        "netbuf.cc",
        "reconnect_transport.cc",
        "rpc_server.cc",
        "rpc_utils.cc",
        "transport.cc",
        "types.cc",
    ],
    hdrs = [
        "include/rpc/backoff_policy.h",
        "include/rpc/connection_cache.h",
        "include/rpc/connection_set.h",
        "include/rpc/errc.h",
        "include/rpc/exceptions.h",
        "include/rpc/fwd.h",
        "include/rpc/logger.h",
        "include/rpc/parse_utils.h",
        "include/rpc/reconnect_transport.h",
        "include/rpc/response_handler.h",
        "include/rpc/rpc_server.h",
        "include/rpc/rpc_utils.h",
        "include/rpc/service.h",
        "include/rpc/transport.h",
        "include/rpc/types.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/bytes:scattered_message",
        "//src/v/compression",
        "//src/v/config",
        "//src/v/hashing:crc32c",
        "//src/v/hashing:xx",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/net",
        "//src/v/reflection:adl",
        "//src/v/serde",
        "//src/v/ssx:future-util",
        "//src/v/ssx:semaphore",
        "//src/v/ssx:sformat",
        "//src/v/utils:log_hist",
        "//src/v/utils:mutex",
        "//src/v/utils:named_type",
        "//src/v/utils:to_string",
        "//src/v/utils:unresolved_address",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "netbuf_test",
    timeout = "short",
    srcs = [
        "test/netbuf_tests.cc",
        "test/test_types.h",
    ],
    deps = [
        ":rpc",
        "//src/v/bytes:iobuf",
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
        "test/roundtrip_tests.cc",
        "test/test_types.h",
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
        "test/response_handler_tests.cc",
    ],
    deps = [
        ":rpc",
        "//src/v/ssx:semaphore",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "serialization_test",
    timeout = "short",
    srcs = [
        "test/serialization_test.cc",
        "test/test_types.h",
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
    src = "test/cycling_service.json",
)

redpanda_cc_rpc_library(
    name = "echo_rpc",
    src = "test/echo_service.json",
)

redpanda_cc_rpc_library(
    name = "echo_v2_rpc",
    src = "test/echo_v2_service.json",
)
