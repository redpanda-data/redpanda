load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "compaction",
    hdrs = [
        "fwd.h",
    ],
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "key",
    hdrs = [
        "key.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "key_offset_map",
    srcs = [
        "key_offset_map.cc",
    ],
    hdrs = [
        "key_offset_map.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":key",
        "//src/v/container:chunked_vector",
        "//src/v/hashing:secure",
        "//src/v/model",
        "//src/v/utils:tracking_allocator",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "types",
    srcs = [
        "types.cc",
    ],
    hdrs = [
        "types.h",
    ],
    implementation_deps = [
        "//src/v/utils:to_string",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":key_offset_map",
        "//src/v/base",
        "//src/v/model",
        "//src/v/storage:file_sanitizer_types",
        "//src/v/storage:scoped_file_tracker",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "utils",
    srcs = [
        "utils.cc",
    ],
    hdrs = [
        "utils.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":key_offset_map",
        "//src/v/config",
        "//src/v/features",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "reducer",
    srcs = [
        "reducer.cc",
    ],
    hdrs = [
        "reducer.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "//src/v/bytes",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "filter",
    srcs = [
        "filter.cc",
    ],
    hdrs = [
        "filter.h",
    ],
    implementation_deps = [
        ":utils",
        "//src/v/model:batch_compression",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":reducer",
        ":types",
        "//src/v/bytes",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "@seastar",
    ],
)
