load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_btest_no_seastar")

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
    ],
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
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
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
        "//src/v/random:generators",
        "@seastar//:benchmark",
    ],
)
