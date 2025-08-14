load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_cc_gtest(
    name = "chunked_hash_map_test",
    timeout = "short",
    srcs = [
        "chunked_hash_map_test.cc",
    ],
    deps = [
        "//src/v/container:chunked_hash_map",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "chunked_vector_test",
    timeout = "short",
    srcs = [
        "chunked_vector_test.cc",
    ],
    deps = [
        "//src/v/container:chunked_vector",
        "//src/v/random:generators",
        "//src/v/serde",
        "//src/v/serde:vector",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "contiguous_range_map_test",
    timeout = "short",
    srcs = [
        "contiguous_range_map_test.cc",
    ],
    deps = [
        "//src/v/container:contiguous_range_map",
        "//src/v/container:zip",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@abseil-cpp//absl/container:btree",
        "@fmt",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "interval_set_test",
    timeout = "short",
    srcs = [
        "interval_set_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/container:interval_set",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "chunked_vector_async_test",
    timeout = "short",
    srcs = [
        "chunked_vector_async_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/test_utils:seastar_boost",
        "@seastar//:testing",
    ],
)

redpanda_test_cc_library(
    name = "bench_utils",
    hdrs = [
        "bench_utils.h",
    ],
    visibility = ["//visibility:private"],
    deps = [
        "//src/v/base",
        "//src/v/random:generators",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "container_rpbench",
    srcs = [
        "map_bench.cc",
        "vector_bench.cc",
    ],
    deps = [
        ":bench_utils",
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/container:contiguous_range_map",
        "//src/v/random:generators",
        "@abseil-cpp//absl/container:btree",
        "@boost//:range",
        "@seastar//:benchmark",
    ],
)
