load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_btest_no_seastar", "redpanda_cc_gtest")

redpanda_cc_library(
    name = "vint",
    srcs = [
        "vint.cc",
    ],
    hdrs = [
        "vint.h",
        "vint_iostream.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iostream",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "functional",
    hdrs = [
        "functional.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "inet_address",
    hdrs = [
        "inet_address_wrapper.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "tristate",
    hdrs = [
        "tristate.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "memory_data_source",
    hdrs = [
        "memory_data_source.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "mutex",
    hdrs = [
        "mutex.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/ssx:semaphore",
    ],
)

redpanda_cc_library(
    name = "static_deleter_fn",
    hdrs = [
        "static_deleter_fn.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
    ],
)

redpanda_cc_library(
    name = "named_type",
    hdrs = [
        "named_type.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
    ],
)

redpanda_cc_library(
    name = "to_string",
    hdrs = [
        "to_string.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
    ],
)

redpanda_cc_library(
    name = "object_pool",
    hdrs = [
        "object_pool.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "human",
    srcs = [
        "human.cc",
    ],
    hdrs = [
        "human.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "unresolved_address",
    hdrs = [
        "unresolved_address.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "uuid",
    srcs = [
        "uuid.cc",
    ],
    hdrs = [
        "uuid.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "@abseil-cpp//absl/hash",
        "@boost//:uuid",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "xid",
    srcs = [
        "xid.cc",
    ],
    hdrs = [
        "xid.h",
    ],
    include_prefix = "utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf_parser",
        "//src/v/serde",
        "//src/v/serde:array",
        "//src/v/ssx:sformat",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_btest_no_seastar(
    name = "tristate_test",
    timeout = "short",
    srcs = [
        "tests/tristate_test.cc",
    ],
    defines = [
        "BOOST_TEST_MODULE=tristate",
    ],
    deps = [
        ":tristate",
    ],
)

redpanda_cc_btest_no_seastar(
    name = "named_type_test",
    timeout = "short",
    srcs = [
        "tests/named_type_tests.cc",
    ],
    defines = [
        "BOOST_TEST_MODULE=named_type",
    ],
    deps = [
        ":named_type",
        "//src/v/base",
    ],
)

redpanda_cc_btest(
    name = "object_pool_test",
    timeout = "short",
    srcs = [
        "tests/object_pool_test.cc",
    ],
    deps = [
        ":object_pool",
        "//src/v/base",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "vint_test",
    timeout = "short",
    srcs = [
        "tests/vint_test.cc",
    ],
    deps = [
        ":vint",
        "//src/v/bytes",
        "//src/v/bytes:iostream",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "uuid_test",
    timeout = "short",
    srcs = [
        "tests/uuid_test.cc",
    ],
    deps = [
        ":named_type",
        ":uuid",
        "//src/v/serde",
        "//src/v/serde:map",
        "//src/v/serde:optional",
        "//src/v/serde:uuid",
        "//src/v/serde:vector",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/hash",
        "@boost//:container_hash",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "xid_test",
    timeout = "short",
    srcs = [
        "tests/xid_test.cc",
    ],
    deps = [
        ":xid",
        "//src/v/random:generators",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
        "@abseil-cpp//absl/container:node_hash_map",
        "@googletest//:gtest",
    ],
)

redpanda_cc_bench(
    name = "vint_bench",
    timeout = "short",
    srcs = [
        "tests/vint_bench.cc",
    ],
    deps = [
        ":vint",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/random:generators",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_btest_no_seastar(
    name = "human_test",
    timeout = "short",
    srcs = [
        "tests/human_test.cc",
    ],
    defines = [
        "BOOST_TEST_MODULE=human",
    ],
    deps = [
        ":human",
        "@boost//:test",
        "@fmt",
    ],
)
