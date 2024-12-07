load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:cert.bzl", "redpanda_selfsigned_cert", "redpanda_signed_cert")
load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_test_cc_library")
load("//src/v/rpc:compiler.bzl", "redpanda_cc_rpc_library")

redpanda_test_cc_library(
    name = "rpc_integration_fixture",
    hdrs = [
        "rpc_integration_fixture.h",
    ],
    include_prefix = "rpc/test",
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/net",
        "//src/v/rpc",
        "@seastar",
    ],
)

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

redpanda_cc_btest(
    name = "exponential_backoff",
    timeout = "short",
    srcs = [
        "exponential_backoff.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/rpc",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "connection_cache_test",
    timeout = "short",
    srcs = [
        "connection_cache_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/base",
        "//src/v/config",
        "//src/v/model",
        "//src/v/rpc",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "rpc_gen_cycling_test",
    timeout = "short",
    srcs = [
        "rpc_gen_cycling_test.cc",
    ],
    cpu = 1,
    data = [
        ":cert",
        ":cert_ca",
        ":other_cert",
        ":other_cert_ca",
    ],
    tags = ["exclusive"],
    deps = [
        ":cycling_rpc",
        ":echo_rpc",
        ":echo_v2_rpc",
        ":rpc_integration_fixture",
        "//src/v/bytes:random",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/rpc",
        "//src/v/test_utils:fixture",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_bench(
    name = "rpc_bench",
    timeout = "short",
    srcs = [
        "rpc_bench.cc",
    ],
    deps = [
        "//src/v/reflection:adl",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_library(
    name = "rpc_gen_types",
    hdrs = [
        "rpc_gen_types.h",
    ],
    include_prefix = "rpc/test",
    deps = [
        "//src/v/base",
        "//src/v/reflection:adl",
        "//src/v/rpc",
        "//src/v/serde",
        "//src/v/serde:enum",
        "//src/v/serde:sstring",
        "@seastar",
    ],
)

redpanda_cc_rpc_library(
    name = "cycling_rpc",
    src = "cycling_service.json",
    out = "cycling_service.h",
    include_prefix = "rpc/test",
    deps = [
        ":rpc_gen_types",
    ],
)

redpanda_cc_rpc_library(
    name = "echo_rpc",
    src = "echo_service.json",
    out = "echo_service.h",
    include_prefix = "rpc/test",
    deps = [
        ":rpc_gen_types",
    ],
)

redpanda_cc_rpc_library(
    name = "echo_v2_rpc",
    src = "echo_v2_service.json",
    out = "echo_v2_service.h",
    include_prefix = "rpc/test",
    deps = [
        ":rpc_gen_types",
    ],
)

redpanda_selfsigned_cert(
    name = "cert_ca",
    certificate = "root_certificate_authority",
    common_name = "redpanda.com",
)

redpanda_selfsigned_cert(
    name = "other_cert_ca",
    certificate = "root_certificate_authority.other",
    common_name = "redpanda.other.com",
)

redpanda_signed_cert(
    name = "cert",
    ca = "root_certificate_authority",
    certificate = "redpanda",
    common_name = "cert.com",
    serial_number = 1,
)

redpanda_signed_cert(
    name = "other_cert",
    ca = "root_certificate_authority.other",
    certificate = "redpanda.other",
    common_name = "cert.other.com",
    serial_number = 2,
)
