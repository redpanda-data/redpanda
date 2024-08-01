load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel/thirdparty:avro.bzl", "avro_cc_library")

package(default_visibility = ["//src/v/iceberg:__subpackages__"])

avro_cc_library(
    name = "manifest_file",
    include_prefix = "iceberg",
    namespace = "iceberg::avrogen",
    schema = "avroschemas/manifest_file.schema.json",
)

avro_cc_library(
    name = "manifest_entry",
    include_prefix = "iceberg",
    namespace = "iceberg::avrogen",
    schema = "avroschemas/manifest_entry.schema.json",
)

redpanda_cc_library(
    name = "avro_utils",
    hdrs = [
        "avro_utils.h",
    ],
    include_prefix = "iceberg",
    deps = [
        "//src/v/bytes:iobuf",
        "@avro",
    ],
)

redpanda_cc_library(
    name = "json_utils",
    srcs = [
        "json_utils.cc",
    ],
    hdrs = [
        "json_utils.h",
    ],
    include_prefix = "iceberg",
    deps = [
        "//src/v/json",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "datatypes",
    srcs = [
        "datatypes.cc",
    ],
    hdrs = [
        "datatypes.h",
    ],
    include_prefix = "iceberg",
    deps = [
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "datatypes_json",
    srcs = [
        "datatypes_json.cc",
    ],
    hdrs = [
        "datatypes_json.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":datatypes",
        ":json_utils",
        "//src/v/json",
        "//src/v/strings:string_switch",
    ],
)

redpanda_cc_library(
    name = "manifest",
    hdrs = [
        "manifest.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":datatypes",
        ":manifest_entry",
        ":partition",
        ":schema",
        "//src/v/container:fragmented_vector",
        "//src/v/utils:named_type",
    ],
)

redpanda_cc_library(
    name = "manifest_avro",
    srcs = [
        "manifest_avro.cc",
    ],
    hdrs = [
        "manifest_avro.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":avro_utils",
        ":datatypes_json",
        ":json_utils",
        ":manifest",
        ":manifest_entry",
        ":partition",
        ":schema",
        ":schema_json",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:fragmented_vector",
        "//src/v/strings:string_switch",
        "//src/v/utils:named_type",
        "@avro",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "partition",
    srcs = [
        "partition.cc",
    ],
    hdrs = [
        "partition.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":datatypes",
        "//src/v/container:fragmented_vector",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "schema",
    hdrs = [
        "schema.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":datatypes",
        "//src/v/container:chunked_hash_map",
        "//src/v/utils:named_type",
    ],
)

redpanda_cc_library(
    name = "schema_json",
    srcs = [
        "schema_json.cc",
    ],
    hdrs = [
        "schema_json.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":datatypes",
        ":datatypes_json",
        ":json_utils",
        ":schema",
        "//src/v/json",
    ],
)
