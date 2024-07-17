load("//bazel:build.bzl", "redpanda_cc_library")

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
