load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "test_schemas",
    srcs = [
        "test_schemas.cc",
    ],
    hdrs = [
        "test_schemas.h",
    ],
    include_prefix = "iceberg/tests",
    deps = [
        "//src/v/iceberg:datatypes",
    ],
)

redpanda_test_cc_library(
    name = "test_table_metadata",
    srcs = [
        "test_table_metadata.cc",
    ],
    hdrs = [
        "test_table_metadata.h",
    ],
    include_prefix = "iceberg/tests",
)

redpanda_test_cc_library(
    name = "value_generator",
    srcs = [
        "value_generator.cc",
    ],
    hdrs = [
        "value_generator.h",
    ],
    include_prefix = "iceberg/tests",
    deps = [
        "//src/v/bytes:random",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:values",
        "//src/v/random:generators",
        "//src/v/test_utils:random",
    ],
)

redpanda_cc_gtest(
    name = "datatypes_test",
    timeout = "short",
    srcs = [
        "datatypes_test.cc",
    ],
    deps = [
        "//src/v/iceberg:datatypes",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "datatypes_json_test",
    timeout = "short",
    srcs = [
        "datatypes_json_test.cc",
    ],
    deps = [
        ":test_schemas",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:datatypes_json",
        "//src/v/iceberg:json_writer",
        "//src/v/json",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "filesystem_catalog_test",
    timeout = "short",
    srcs = [
        "filesystem_catalog_test.cc",
    ],
    cpu = 1,
    deps = [
        ":test_schemas",
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage/tests:s3_imposter_gtest",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:filesystem_catalog",
        "//src/v/iceberg:logger",
        "//src/v/iceberg:schema",
        "//src/v/iceberg:table_metadata_json",
        "//src/v/iceberg:transform",
        "//src/v/json",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "manifest_entry_type_test",
    timeout = "short",
    srcs = [
        "manifest_entry_type_test.cc",
    ],
    deps = [
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:manifest_entry_type",
        "//src/v/iceberg:partition_key_type",
        "//src/v/iceberg:schema_avro",
        "//src/v/test_utils:gtest",
        "@avro",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "manifest_file_packer_test",
    timeout = "short",
    srcs = [
        "manifest_file_packer_test.cc",
    ],
    deps = [
        "//src/v/container:fragmented_vector",
        "//src/v/iceberg:manifest_file_packer",
        "//src/v/iceberg:manifest_list",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "manifest_io_test",
    timeout = "short",
    srcs = [
        "manifest_io_test.cc",
    ],
    cpu = 1,
    deps = [
        ":test_schemas",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io:transfer_details",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage/tests:s3_imposter_gtest",
        "//src/v/iceberg:manifest",
        "//src/v/iceberg:manifest_io",
        "//src/v/iceberg:manifest_list",
        "//src/v/test_utils:gtest",
        "//src/v/utils:retry_chain_node",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "manifest_serialization_test",
    timeout = "short",
    srcs = [
        "manifest_serialization_test.cc",
    ],
    data = [
        "testdata/nested_manifest.avro",
    ],
    deps = [
        ":test_schemas",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:fragmented_vector",
        "//src/v/iceberg:avro_utils",
        "//src/v/iceberg:manifest",
        "//src/v/iceberg:manifest_avro",
        "//src/v/iceberg:manifest_entry_gen",
        "//src/v/iceberg:manifest_file_gen",
        "//src/v/iceberg:manifest_list",
        "//src/v/iceberg:manifest_list_avro",
        "//src/v/iceberg:partition_key_type",
        "//src/v/iceberg:schema_json",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:runfiles",
        "//src/v/utils:file_io",
        "@avro",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "merge_append_action_test",
    timeout = "short",
    srcs = [
        "merge_append_action_test.cc",
    ],
    cpu = 1,
    deps = [
        ":test_schemas",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage/tests:s3_imposter_gtest",
        "//src/v/iceberg:manifest",
        "//src/v/iceberg:manifest_entry",
        "//src/v/iceberg:manifest_io",
        "//src/v/iceberg:merge_append_action",
        "//src/v/iceberg:partition",
        "//src/v/iceberg:transaction",
        "//src/v/iceberg:values_bytes",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "partition_json_test",
    timeout = "short",
    srcs = [
        "partition_json_test.cc",
    ],
    deps = [
        "//src/v/iceberg:json_writer",
        "//src/v/iceberg:partition",
        "//src/v/iceberg:partition_json",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "partition_test",
    timeout = "short",
    srcs = [
        "partition_test.cc",
    ],
    deps = [
        "//src/v/container:fragmented_vector",
        "//src/v/iceberg:partition",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "partition_key_test",
    timeout = "short",
    srcs = [
        "partition_key_test.cc",
    ],
    deps = [
        ":test_schemas",
        ":value_generator",
        "//src/v/iceberg:partition",
        "//src/v/iceberg:partition_key",
        "//src/v/iceberg:struct_accessor",
        "//src/v/iceberg:transform",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "partition_key_type_test",
    timeout = "short",
    srcs = [
        "partition_key_type_test.cc",
    ],
    deps = [
        ":test_schemas",
        "//src/v/iceberg:partition",
        "//src/v/iceberg:partition_key_type",
        "//src/v/iceberg:schema",
        "//src/v/iceberg:transform",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "schema_avro_test",
    timeout = "short",
    srcs = [
        "schema_avro_test.cc",
    ],
    deps = [
        ":test_schemas",
        "//src/v/iceberg:schema",
        "//src/v/iceberg:schema_avro",
        "//src/v/test_utils:gtest",
        "@avro",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "schema_json_test",
    timeout = "short",
    srcs = [
        "schema_json_test.cc",
    ],
    deps = [
        ":test_schemas",
        "//src/v/iceberg:json_writer",
        "//src/v/iceberg:schema",
        "//src/v/iceberg:schema_json",
        "//src/v/json",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "schema_test",
    timeout = "short",
    srcs = [
        "schema_test.cc",
    ],
    deps = [
        ":test_schemas",
        "//src/v/iceberg:schema",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "snapshot_json_test",
    timeout = "short",
    srcs = [
        "snapshot_json_test.cc",
    ],
    deps = [
        "//src/v/iceberg:json_writer",
        "//src/v/iceberg:snapshot",
        "//src/v/iceberg:snapshot_json",
        "//src/v/json",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "struct_accessor_test",
    timeout = "short",
    srcs = [
        "struct_accessor_test.cc",
    ],
    deps = [
        ":test_schemas",
        ":value_generator",
        "//src/v/iceberg:struct_accessor",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "table_io_test",
    timeout = "short",
    srcs = [
        "table_io_test.cc",
    ],
    cpu = 1,
    deps = [
        ":test_table_metadata",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage/tests:s3_imposter_gtest",
        "//src/v/iceberg:table_io",
        "//src/v/iceberg:table_metadata",
        "//src/v/iceberg:table_metadata_json",
        "//src/v/json",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "table_metadata_json_test",
    timeout = "short",
    srcs = [
        "table_metadata_json_test.cc",
    ],
    deps = [
        ":test_table_metadata",
        "//src/v/iceberg:json_writer",
        "//src/v/iceberg:table_metadata",
        "//src/v/iceberg:table_metadata_json",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "transform_json_test",
    timeout = "short",
    srcs = [
        "transform_json_test.cc",
    ],
    deps = [
        "//src/v/iceberg:transform",
        "//src/v/iceberg:transform_json",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "table_update_applier_test",
    timeout = "short",
    srcs = [
        "table_update_applier_test.cc",
    ],
    deps = [
        ":test_schemas",
        "//src/v/iceberg:table_metadata",
        "//src/v/iceberg:table_update",
        "//src/v/iceberg:table_update_applier",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "transform_utils_test",
    timeout = "short",
    srcs = [
        "transform_utils_test.cc",
    ],
    deps = [
        "//src/v/iceberg:transform",
        "//src/v/iceberg:transform_utils",
        "//src/v/iceberg:values",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "update_schema_action_test",
    timeout = "short",
    srcs = [
        "update_schema_action_test.cc",
    ],
    cpu = 1,
    deps = [
        ":test_schemas",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage/tests:s3_imposter_gtest",
        "//src/v/iceberg:manifest_io",
        "//src/v/iceberg:transaction",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "values_avro_test",
    timeout = "short",
    srcs = [
        "values_avro_test.cc",
    ],
    deps = [
        ":test_schemas",
        ":value_generator",
        "//src/v/iceberg:schema_avro",
        "//src/v/iceberg:values",
        "//src/v/iceberg:values_avro",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "values_bytes_test",
    timeout = "short",
    srcs = [
        "values_bytes_test.cc",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/iceberg:values",
        "//src/v/iceberg:values_bytes",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "values_test",
    timeout = "short",
    srcs = [
        "values_test.cc",
    ],
    deps = [
        ":test_schemas",
        ":value_generator",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:fragmented_vector",
        "//src/v/iceberg:values",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "metadata_query_test",
    timeout = "short",
    srcs = [
        "metadata_query_test.cc",
    ],
    cpu = 1,
    deps = [
        ":test_schemas",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage/tests:s3_imposter_gtest",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:fragmented_vector",
        "//src/v/iceberg:merge_append_action",
        "//src/v/iceberg:metadata_query",
        "//src/v/iceberg:partition",
        "//src/v/iceberg:table_io",
        "//src/v/iceberg:table_metadata",
        "//src/v/iceberg:table_metadata_json",
        "//src/v/iceberg:transaction",
        "//src/v/iceberg:values",
        "//src/v/iceberg:values_bytes",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
