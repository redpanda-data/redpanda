load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = ["//src/v/serde/parquet:__subpackages__"],
)

redpanda_cc_library(
    name = "schema",
    srcs = [
        "flattened_schema.cc",
        "schema.cc",
    ],
    hdrs = [
        "flattened_schema.h",
        "schema.h",
    ],
    include_prefix = "serde/parquet",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "metadata",
    srcs = [
        "metadata.cc",
    ],
    hdrs = [
        "metadata.h",
    ],
    include_prefix = "serde/parquet",
    deps = [
        ":schema",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:fragmented_vector",
        "//src/v/hashing:crc32",
        "//src/v/serde/thrift:compact",
        "//src/v/utils:vint",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "value",
    srcs = [
        "value.cc",
    ],
    hdrs = [
        "value.h",
    ],
    implementation_deps = [
        "@abseil-cpp//absl/strings",
        "@seastar",
    ],
    include_prefix = "serde/parquet",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:fragmented_vector",
    ],
)

redpanda_cc_library(
    name = "encoding",
    srcs = [
        "encoding.cc",
    ],
    hdrs = [
        "encoding.h",
    ],
    include_prefix = "serde/parquet",
    deps = [
        ":schema",
        ":value",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:fragmented_vector",
        "//src/v/utils:vint",
    ],
)

redpanda_cc_library(
    name = "shredder",
    srcs = [
        "shredder.cc",
    ],
    hdrs = [
        "shredder.h",
    ],
    include_prefix = "serde/parquet",
    deps = [
        ":schema",
        ":value",
        "//src/v/base",
        "@abseil-cpp//absl/functional:function_ref",
        "@boost//:range",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "column_writer",
    srcs = [
        "column_stats_collector.cc",
        "column_writer.cc",
    ],
    hdrs = [
        "column_stats_collector.h",
        "column_writer.h",
    ],
    include_prefix = "serde/parquet",
    deps = [
        ":encoding",
        ":metadata",
        ":schema",
        ":value",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/compression",
        "//src/v/hashing:crc32",
        "@abseil-cpp//absl/numeric:int128",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "writer",
    srcs = [
        "writer.cc",
    ],
    hdrs = [
        "writer.h",
    ],
    implementation_deps = [
        ":column_writer",
        ":metadata",
        ":shredder",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/container:contiguous_range_map",
    ],
    include_prefix = "serde/parquet",
    visibility = ["//visibility:public"],
    deps = [
        ":schema",
        ":value",
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "@seastar",
    ],
)
