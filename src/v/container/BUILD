load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_gtest")

redpanda_cc_library(
    name = "intrusive",
    hdrs = [
        "include/container/intrusive_list_helpers.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "@boost//:intrusive",
    ],
)

redpanda_cc_library(
    name = "fragmented_vector",
    hdrs = [
        "include/container/fragmented_vector.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "chunked_hash_map",
    hdrs = [
        "include/container/chunked_hash_map.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        ":fragmented_vector",
        "@abseil-cpp//absl/hash",
        "@unordered_dense",
    ],
)

redpanda_cc_library(
    name = "interval_set",
    hdrs = [
        "include/container/interval_set.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "contiguous_range_map",
    hdrs = [
        "include/container/contiguous_range_map.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        ":fragmented_vector",
    ],
)

redpanda_cc_library(
    name = "zip",
    hdrs = [
        "include/container/zip.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
)

redpanda_cc_gtest(
    name = "chunked_hash_map_test",
    timeout = "short",
    srcs = [
        "tests/chunked_hash_map_test.cc",
    ],
    deps = [
        ":chunked_hash_map",
        "//src/v/test_utils:gtest",
    ],
)

redpanda_cc_gtest(
    name = "fragmented_vector_test",
    timeout = "short",
    srcs = [
        "tests/fragmented_vector_test.cc",
    ],
    deps = [
        ":fragmented_vector",
        "//src/v/random:generators",
        "//src/v/serde",
        "//src/v/test_utils:gtest",
    ],
)

redpanda_cc_gtest(
    name = "contiguous_range_map_test",
    timeout = "short",
    srcs = [
        "tests/contiguous_range_map_test.cc",
    ],
    deps = [
        ":contiguous_range_map",
        ":zip",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
    ],
)

redpanda_cc_gtest(
    name = "interval_set_test",
    timeout = "short",
    srcs = [
        "tests/interval_set_test.cc",
    ],
    deps = [
        ":interval_set",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
    ],
)

redpanda_cc_btest(
    name = "fragmented_vector_async_test",
    timeout = "short",
    srcs = [
        "tests/fragmented_vector_async_test.cc",
    ],
    deps = [
        ":fragmented_vector",
        "//src/v/base",
        "//src/v/test_utils:seastar_boost",
    ],
)

redpanda_cc_library(
    name = "bench_utils",
    hdrs = [
        "tests/bench_utils.h",
    ],
    include_prefix = "container",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/random:generators",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "vector_bench",
    timeout = "short",
    srcs = [
        "tests/vector_bench.cc",
    ],
    deps = [
        ":bench_utils",
        ":fragmented_vector",
    ],
)

redpanda_cc_bench(
    name = "map_bench",
    timeout = "short",
    srcs = [
        "tests/map_bench.cc",
    ],
    deps = [
        ":bench_utils",
        ":contiguous_range_map",
    ],
)
