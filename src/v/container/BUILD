load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "intrusive",
    hdrs = [
        "intrusive_list_helpers.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@boost//:intrusive",
    ],
)

redpanda_cc_library(
    name = "chunked_vector",
    hdrs = [
        "chunked_vector.h",
        "chunked_vector_async.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "chunked_hash_map",
    hdrs = [
        "chunked_hash_map.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":chunked_vector",
        "@abseil-cpp//absl/hash",
        "@unordered_dense",
    ],
)

redpanda_cc_library(
    name = "interval_set",
    hdrs = [
        "interval_set.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf_parser",
        "//src/v/serde",
        "//src/v/serde:map",
        "//src/v/utils:to_string",
        "@abseil-cpp//absl/container:btree",
    ],
)

redpanda_cc_library(
    name = "contiguous_range_map",
    hdrs = [
        "contiguous_range_map.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":chunked_vector",
    ],
)

redpanda_cc_library(
    name = "zip",
    hdrs = [
        "zip.h",
    ],
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
    visibility = ["//visibility:public"],
    deps = [
        ":chunked_vector",
        "//src/v/json",
    ],
)

redpanda_cc_library(
    name = "lw_shared_container",
    hdrs = [
        "lw_shared_container.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "chunked_circular_buffer",
    hdrs = [
        "chunked_circular_buffer.h",
    ],
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "priority_queue",
    hdrs = [
        "priority_queue.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":chunked_vector",
        "//src/v/base",
        "//src/v/ssx:async_algorithm",
    ],
)
