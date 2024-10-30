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
    deps = [
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "//src/v/utils:uuid",
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
        "//src/v/serde/thrift:compact",
        "//src/v/utils:uuid",
        "//src/v/utils:vint",
        "@abseil-cpp//absl/container:flat_hash_map",
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
    include_prefix = "serde/parquet",
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:fragmented_vector",
        "//src/v/utils:base64",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/numeric:int128",
        "@seastar",
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
        "@boost//:range",
        "@seastar",
    ],
)
