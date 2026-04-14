load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = ["//src/v/serde/parquet:__subpackages__"],
)

redpanda_cc_library(
    name = "file_io",
    srcs = ["file_io.cc"],
    hdrs = ["file_io.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
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
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_vector",
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
    visibility = ["//visibility:public"],
    deps = [
        ":schema",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:chunked_vector",
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
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
    ],
)

redpanda_cc_library(
    name = "reader",
    srcs = [
        "reader.cc",
    ],
    hdrs = [
        "reader.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":assembler",
        ":column_array",
        ":column_chunk_reader",
        ":metadata",
        ":schema",
        ":value",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:chunked_vector",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "assembler",
    srcs = [
        "assembler.cc",
    ],
    hdrs = [
        "assembler.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":column_array",
        ":schema",
        ":value",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "column_chunk_reader",
    srcs = [
        "column_chunk_reader.cc",
    ],
    hdrs = [
        "column_chunk_reader.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":column_array",
        ":encoding",
        ":metadata",
        ":schema",
        ":value",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/compression",
        "//src/v/container:chunked_vector",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "column_array",
    srcs = [
        "column_array.cc",
    ],
    hdrs = [
        "column_array.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":schema",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
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
    visibility = ["//visibility:public"],
    deps = [
        ":schema",
        ":value",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:chunked_vector",
        "//src/v/utils:vint",
        "@seastar",
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
    deps = [
        ":encoding",
        ":metadata",
        ":schema",
        ":value",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/compression",
        "//src/v/container:chunked_vector",
        "//src/v/hashing:crc32",
        "@abseil-cpp//absl/numeric:int128",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "variant_value",
    srcs = ["variant_value.cc"],
    hdrs = ["variant_value.h"],
    deps = [
        ":value",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "variant_json",
    srcs = ["variant_json.cc"],
    hdrs = ["variant_json.h"],
    implementation_deps = [
        "@fmt",
    ],
    deps = [
        ":variant_value",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/serde/json:parser",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "variant_shredder",
    srcs = ["variant_shredder.cc"],
    hdrs = ["variant_shredder.h"],
    deps = [
        ":schema",
        ":value",
        ":variant_value",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "variant_encoding",
    srcs = ["variant_encoding.cc"],
    hdrs = ["variant_encoding.h"],
    deps = [
        ":value",
        ":variant_value",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:chunked_vector",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "variant_schema",
    srcs = ["variant_schema.cc"],
    hdrs = ["variant_schema.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":schema",
        ":value",
        ":variant_encoding",
        ":variant_shredder",
        ":variant_value",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
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
        ":shredder",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/container:contiguous_range_map",
        "@abseil-cpp//absl/container:flat_hash_set",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":metadata",
        ":schema",
        ":value",
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "@seastar",
    ],
)
