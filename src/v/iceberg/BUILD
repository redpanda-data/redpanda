load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel/thirdparty:avro.bzl", "avro_cc_library")

package(default_visibility = ["//src/v/iceberg:__subpackages__"])

avro_cc_library(
    name = "manifest_file_gen",
    class_name = "manifest_file",
    include_prefix = "iceberg",
    namespace = "iceberg::avrogen",
    schema = "avroschemas/manifest_file.schema.json",
)

avro_cc_library(
    name = "manifest_entry_gen",
    class_name = "manifest_entry",
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
        ":manifest_entry_gen",
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
        ":manifest_entry_gen",
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
    name = "manifest_entry",
    hdrs = [
        "manifest_entry.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":partition_key",
        ":values",
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "partition",
    hdrs = [
        "partition.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":datatypes",
        ":transform",
        "//src/v/container:fragmented_vector",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "partition_key",
    srcs = [
        "partition_key.cc",
    ],
    hdrs = [
        "partition_key.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":partition",
        ":struct_accessor",
        ":transform_utils",
        ":values",
    ],
)

redpanda_cc_library(
    name = "partition_key_type",
    srcs = [
        "partition_key_type.cc",
    ],
    hdrs = [
        "partition_key_type.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":datatypes",
        ":partition",
        ":schema",
    ],
)

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    include_prefix = "iceberg",
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "schema",
    srcs = [
        "schema.cc",
    ],
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
    name = "struct_accessor",
    srcs = [
        "struct_accessor.cc",
    ],
    hdrs = [
        "struct_accessor.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":datatypes",
        ":values",
        "//src/v/container:chunked_hash_map",
    ],
)

redpanda_cc_library(
    name = "schema_avro",
    srcs = [
        "schema_avro.cc",
    ],
    hdrs = [
        "schema_avro.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":datatypes",
        "@avro",
        "@fmt",
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

redpanda_cc_library(
    name = "time_transform_visitor",
    srcs = [
        "time_transform_visitor.cc",
    ],
    hdrs = [
        "time_transform_visitor.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":values",
    ],
)

redpanda_cc_library(
    name = "transform",
    srcs = [
        "transform.cc",
    ],
    hdrs = [
        "transform.h",
    ],
    include_prefix = "iceberg",
)

redpanda_cc_library(
    name = "transform_utils",
    srcs = [
        "transform_utils.cc",
    ],
    hdrs = [
        "transform_utils.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":time_transform_visitor",
        ":transform",
        ":values",
    ],
)

redpanda_cc_library(
    name = "values",
    srcs = [
        "values.cc",
    ],
    hdrs = [
        "values.h",
    ],
    implementation_deps = [
        "//src/v/bytes:hash",
        "//src/v/bytes:iobuf_parser",
    ],
    include_prefix = "iceberg",
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/container:fragmented_vector",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/numeric:int128",
        "@boost//:container_hash",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "values_avro",
    srcs = [
        "values_avro.cc",
    ],
    hdrs = [
        "values_avro.h",
    ],
    implementation_deps = [
        "//src/v/bytes:iobuf_parser",
    ],
    include_prefix = "iceberg",
    deps = [
        ":datatypes",
        ":values",
        "@avro",
    ],
)
