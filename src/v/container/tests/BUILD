load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "bench_utils",
    hdrs = [
        "bench_utils.h",
    ],
    include_prefix = "container/tests",
    visibility = ["//visibility:private"],
    deps = [
        "//src/v/base",
        "//src/v/random:generators",
        "@seastar//:benchmark",
    ],
)

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
    name = "fragmented_vector_test",
    timeout = "short",
    srcs = [
        "fragmented_vector_test.cc",
    ],
    deps = [
        "//src/v/container:fragmented_vector",
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
    name = "fragmented_vector_async_test",
    timeout = "short",
    srcs = [
        "fragmented_vector_async_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "//src/v/test_utils:seastar_boost",
        "@seastar//:testing",
    ],
)

redpanda_cc_bench(
    name = "vector_bench",
    timeout = "short",
    srcs = [
        "vector_bench.cc",
    ],
    deps = [
        ":bench_utils",
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "//src/v/random:generators",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "map_bench",
    timeout = "short",
    srcs = [
        "map_bench.cc",
    ],
    deps = [
        ":bench_utils",
        "//src/v/container:contiguous_range_map",
        "//src/v/random:generators",
        "@abseil-cpp//absl/container:btree",
        "@boost//:range",
        "@seastar//:benchmark",
    ],
)
