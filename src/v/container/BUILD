load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "intrusive",
    hdrs = [
        "intrusive_list_helpers.h",
    ],
    include_prefix = "container",
    visibility = ["//visibility:public"],
    deps = [
        "@boost//:intrusive",
    ],
)

redpanda_cc_library(
    name = "fragmented_vector",
    hdrs = [
        "fragmented_vector.h",
    ],
    include_prefix = "container",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "chunked_hash_map",
    hdrs = [
        "chunked_hash_map.h",
    ],
    include_prefix = "container",
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
        "interval_set.h",
    ],
    include_prefix = "container",
    visibility = ["//visibility:public"],
    deps = [
        "@abseil-cpp//absl/container:btree",
    ],
)

redpanda_cc_library(
    name = "contiguous_range_map",
    hdrs = [
        "contiguous_range_map.h",
    ],
    include_prefix = "container",
    visibility = ["//visibility:public"],
    deps = [
        ":fragmented_vector",
    ],
)

redpanda_cc_library(
    name = "zip",
    hdrs = [
        "zip.h",
    ],
    include_prefix = "container",
    visibility = ["//visibility:public"],
    deps = [
        "@boost//:iterator",
    ],
)

redpanda_cc_library(
    name = "json",
    hdrs = [
        "json.h",
    ],
    include_prefix = "container",
    visibility = ["//visibility:public"],
    deps = [
        ":fragmented_vector",
        "//src/v/json",
    ],
)

redpanda_cc_library(
    name = "lw_shared_container",
    hdrs = [
        "lw_shared_container.h",
    ],
    include_prefix = "container",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)
