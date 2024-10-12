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
    name = "action",
    srcs = [
        "action.cc",
    ],
    hdrs = [
        "action.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":table_requirement",
        ":table_update",
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "avro_utils",
    hdrs = [
        "avro_utils.h",
    ],
    include_prefix = "iceberg",
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/container:fragmented_vector",
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
    name = "json_writer",
    hdrs = [
        "json_writer.h",
    ],
    include_prefix = "iceberg",
    deps = [
        "//src/v/json",
        "//src/v/strings:utf8",
    ],
)

redpanda_cc_library(
    name = "catalog",
    srcs = [
        "catalog.cc",
    ],
    hdrs = [
        "catalog.h",
    ],
    implementation_deps = [
        "@fmt",
    ],
    include_prefix = "iceberg",
    visibility = ["//visibility:public"],
    deps = [
        ":partition",
        ":schema",
        ":transaction",
        "//src/v/base",
        "@seastar",
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
    visibility = ["//visibility:public"],
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
        ":json_writer",
        "//src/v/json",
        "//src/v/strings:string_switch",
    ],
)

redpanda_cc_library(
    name = "filesystem_catalog",
    srcs = [
        "filesystem_catalog.cc",
    ],
    hdrs = [
        "filesystem_catalog.h",
    ],
    implementation_deps = [
        ":manifest_entry",
    ],
    include_prefix = "iceberg",
    visibility = ["//visibility:public"],
    deps = [
        ":catalog",
        ":datatypes",
        ":partition",
        ":schema",
        ":table_io",
        ":table_metadata",
        ":table_update_applier",
        ":transaction",
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/model",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "manifest",
    srcs = [
        "manifest.cc",
    ],
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
        ":manifest_entry_type",
        ":manifest_entry_values",
        ":partition",
        ":partition_json",
        ":partition_key_type",
        ":schema",
        ":schema_avro",
        ":schema_json",
        ":values_avro",
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
    srcs = [
        "manifest_entry.cc",
    ],
    hdrs = [
        "manifest_entry.h",
    ],
    include_prefix = "iceberg",
    visibility = ["//visibility:public"],
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
    name = "manifest_entry_type",
    srcs = [
        "manifest_entry_type.cc",
    ],
    hdrs = [
        "manifest_entry_type.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":datatypes",
        ":partition_key_type",
    ],
)

redpanda_cc_library(
    name = "manifest_entry_values",
    srcs = [
        "manifest_entry_values.cc",
    ],
    hdrs = [
        "manifest_entry_values.h",
    ],
    implementation_deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/strings:string_switch",
    ],
    include_prefix = "iceberg",
    deps = [
        ":manifest_entry",
        ":partition_key_type",
        ":values",
    ],
)

redpanda_cc_library(
    name = "manifest_file_packer",
    srcs = [
        "manifest_file_packer.cc",
    ],
    hdrs = [
        "manifest_file_packer.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":manifest_list",
        "//src/v/container:fragmented_vector",
    ],
)

redpanda_cc_library(
    name = "manifest_io",
    srcs = [
        "manifest_io.cc",
    ],
    hdrs = [
        "manifest_io.h",
    ],
    implementation_deps = [
        ":logger",
        ":manifest_avro",
        ":manifest_list_avro",
        "//src/v/bytes:iobuf",
        "//src/v/ssx:future_util",
    ],
    include_prefix = "iceberg",
    visibility = ["//visibility:public"],
    deps = [
        ":manifest",
        ":manifest_list",
        ":metadata_io",
        ":partition_key_type",
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "manifest_list",
    hdrs = [
        "manifest_list.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":manifest_entry",
        ":partition",
        "//src/v/bytes",
        "//src/v/container:fragmented_vector",
        "//src/v/utils:named_type",
    ],
)

redpanda_cc_library(
    name = "manifest_list_avro",
    srcs = [
        "manifest_list_avro.cc",
    ],
    hdrs = [
        "manifest_list_avro.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/iceberg:avro_utils",
        "//src/v/iceberg:manifest_file_gen",
        "//src/v/strings:string_switch",
        "@avro",
    ],
    include_prefix = "iceberg",
    deps = [
        ":manifest_list",
        "//src/v/bytes:iobuf",
    ],
)

redpanda_cc_library(
    name = "merge_append_action",
    srcs = [
        "merge_append_action.cc",
    ],
    hdrs = [
        "merge_append_action.h",
    ],
    implementation_deps = [
        ":logger",
        ":manifest",
        ":manifest_file_packer",
        ":snapshot",
        ":table_requirement",
        "//src/v/random:generators",
    ],
    include_prefix = "iceberg",
    deps = [
        ":action",
        ":manifest_entry",
        ":manifest_io",
        ":manifest_list",
        ":schema",
        ":table_metadata",
        ":values_bytes",
        "//src/v/base",
    ],
)

redpanda_cc_library(
    name = "metadata_io",
    hdrs = [
        "metadata_io.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":logger",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
        "//src/v/ssx:future_util",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "partition",
    hdrs = [
        "partition.h",
    ],
    include_prefix = "iceberg",
    visibility = ["//visibility:public"],
    deps = [
        ":datatypes",
        ":transform",
        "//src/v/container:fragmented_vector",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "partition_json",
    srcs = [
        "partition_json.cc",
    ],
    hdrs = [
        "partition_json.h",
    ],
    implementation_deps = [
        ":json_utils",
        ":json_writer",
        ":transform_json",
        "//src/v/strings:string_switch",
    ],
    include_prefix = "iceberg",
    deps = [
        ":partition",
        "//src/v/json",
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
    visibility = ["//visibility:public"],
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
    visibility = ["//visibility:public"],
    deps = [
        ":datatypes",
        "//src/v/container:chunked_hash_map",
        "//src/v/utils:named_type",
        "@abseil-cpp//absl/container:btree",
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
    implementation_deps = [
        "@abseil-cpp//absl/container:btree",
    ],
    include_prefix = "iceberg",
    deps = [
        ":datatypes",
        ":datatypes_json",
        ":json_utils",
        ":json_writer",
        ":schema",
        "//src/v/json",
    ],
)

redpanda_cc_library(
    name = "snapshot",
    hdrs = [
        "snapshot.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":manifest_entry",
        ":schema",
        "//src/v/model",
        "@abseil-cpp//absl/container:btree",
    ],
)

redpanda_cc_library(
    name = "snapshot_json",
    srcs = [
        "snapshot_json.cc",
    ],
    hdrs = [
        "snapshot_json.h",
    ],
    implementation_deps = [
        ":json_utils",
        ":json_writer",
        "//src/v/model",
        "//src/v/strings:string_switch",
        "@abseil-cpp//absl/container:btree",
    ],
    include_prefix = "iceberg",
    deps = [
        ":snapshot",
        "//src/v/json",
    ],
)

redpanda_cc_library(
    name = "table_io",
    srcs = [
        "table_io.cc",
    ],
    hdrs = [
        "table_io.h",
    ],
    implementation_deps = [
        ":table_metadata_json",
        "//src/v/bytes:iobuf_parser",
        "//src/v/json",
    ],
    include_prefix = "iceberg",
    deps = [
        ":metadata_io",
        ":table_metadata",
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "table_metadata",
    hdrs = [
        "table_metadata.h",
    ],
    include_prefix = "iceberg",
    visibility = ["//visibility:public"],
    deps = [
        ":manifest",
        ":manifest_entry",
        ":partition",
        ":schema",
        ":snapshot",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
    ],
)

redpanda_cc_library(
    name = "table_metadata_json",
    srcs = [
        "table_metadata_json.cc",
    ],
    hdrs = [
        "table_metadata_json.h",
    ],
    implementation_deps = [
        ":json_utils",
        ":partition_json",
        ":schema_json",
        ":snapshot",
        ":snapshot_json",
        ":transform_json",
        "//src/v/strings:string_switch",
    ],
    include_prefix = "iceberg",
    deps = [
        ":json_writer",
        ":table_metadata",
        "//src/v/json",
    ],
)

redpanda_cc_library(
    name = "table_requests",
    hdrs = [
        "table_requests.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":partition",
        ":schema",
        ":table_requirement",
        ":table_update",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:fragmented_vector",
        "//src/v/utils:uuid",
    ],
)

redpanda_cc_library(
    name = "table_requirement",
    hdrs = [
        "table_requirement.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":datatypes",
        ":manifest_entry",
        ":partition",
        ":schema",
        "//src/v/utils:uuid",
    ],
)

redpanda_cc_library(
    name = "table_update",
    hdrs = [
        "table_update.h",
    ],
    include_prefix = "iceberg",
    deps = [
        ":datatypes",
        ":schema",
        ":snapshot",
        "//src/v/container:fragmented_vector",
        "//src/v/utils:uuid",
    ],
)

redpanda_cc_library(
    name = "table_update_applier",
    srcs = [
        "table_update_applier.cc",
    ],
    hdrs = [
        "table_update_applier.h",
    ],
    implementation_deps = [
        ":logger",
        ":schema",
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:fragmented_vector",
    ],
    include_prefix = "iceberg",
    deps = [
        ":table_metadata",
        ":table_update",
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
    name = "transaction",
    srcs = [
        "transaction.cc",
    ],
    hdrs = [
        "transaction.h",
    ],
    implementation_deps = [
        ":table_update_applier",
        ":update_schema_action",
    ],
    include_prefix = "iceberg",
    visibility = ["//visibility:public"],
    deps = [
        ":action",
        ":manifest_io",
        ":merge_append_action",
        ":schema",
        ":table_metadata",
        ":table_requirement",
        ":table_update",
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "@seastar",
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
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "transform_json",
    srcs = [
        "transform_json.cc",
    ],
    hdrs = [
        "transform_json.h",
    ],
    implementation_deps = [
        ":json_utils",
        "//src/v/strings:string_switch",
    ],
    include_prefix = "iceberg",
    deps = [
        ":transform",
        "//src/v/base",
        "@seastar",
    ],
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
    name = "update_schema_action",
    srcs = [
        "update_schema_action.cc",
    ],
    hdrs = [
        "update_schema_action.h",
    ],
    implementation_deps = [
        ":table_requirement",
        ":table_update",
    ],
    include_prefix = "iceberg",
    deps = [
        ":action",
        ":schema",
        ":table_metadata",
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
    visibility = ["//visibility:public"],
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

redpanda_cc_library(
    name = "values_bytes",
    srcs = [
        "values_bytes.cc",
    ],
    hdrs = [
        "values_bytes.h",
    ],
    implementation_deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/utils:uuid",
    ],
    include_prefix = "iceberg",
    visibility = ["//visibility:public"],
    deps = [
        ":datatypes",
        ":values",
        "//src/v/bytes",
    ],
)

redpanda_cc_library(
    name = "metadata_query",
    srcs = [
        "metadata_query.cc",
    ],
    hdrs = [
        "metadata_query.h",
    ],
    implementation_deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/utils:uuid",
    ],
    include_prefix = "iceberg",
    visibility = ["//visibility:public"],
    deps = [
        ":manifest_io",
        ":schema",
        ":table_metadata",
        ":table_requirement",
        ":table_update",
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "@seastar",
    ],
)
