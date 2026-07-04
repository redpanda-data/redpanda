load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_fuzz_test", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_cc_gtest(
    name = "radix_tree_test",
    timeout = "short",
    srcs = [
        "radix_tree_test.cc",
    ],
    deps = [
        "//src/v/container:radix_tree",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_fuzz_test(
    name = "radix_tree_fuzz",
    timeout = "short",
    srcs = [
        "radix_tree_fuzz.cc",
    ],
    args = [
        "-max_total_time=30",
        "-rss_limit_mb=8192",
    ],
    deps = [
        "//src/v/container:radix_tree",
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

redpanda_test_cc_library(
    name = "bench_utils",
    hdrs = [
        "bench_utils.h",
    ],
    visibility = ["//visibility:private"],
    deps = [
        "//src/v/base",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "radix_tree_rpbench",
    srcs = [
        "radix_tree_bench.cc",
    ],
    deps = [
        "//src/v/container:radix_tree",
        "//src/v/random:generators",
        "@fmt",
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

redpanda_cc_bench(
    name = "absl_map_rpbench",
    srcs = [
        "absl_map_bench.cc",
    ],
    deps = [
        ":bench_utils",
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/container:contiguous_range_map",
        "//src/v/random:generators",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:node_hash_map",
        "@boost//:range",
        "@fmt",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_gtest(
    name = "priority_queue_test",
    timeout = "short",
    srcs = [
        "priority_queue_test.cc",
    ],
    deps = [
        "//src/v/container:priority_queue",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_bench(
    name = "priority_queue_rpbench",
    srcs = [
        "priority_queue_bench.cc",
    ],
    deps = [
        ":bench_utils",
        "//src/v/container:chunked_vector",
        "//src/v/container:priority_queue",
        "//src/v/random:generators",
        "//src/v/ssx:async_algorithm",
        "//src/v/ssx:async_clear",
        "//src/v/ssx:async_merge_top_k",
        "@seastar",
        "@seastar//:benchmark",
    ],
)
