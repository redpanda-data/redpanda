load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "catalog_and_registry_fixture",
    hdrs = ["catalog_and_registry_fixture.h"],
    include_prefix = "datalake/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/iceberg:filesystem_catalog",
        "//src/v/schema/tests:fake_registry",
    ],
)

redpanda_test_cc_library(
    name = "test_data",
    hdrs = ["test_data.h"],
    include_prefix = "datalake/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/iceberg:datatypes",
    ],
)

redpanda_test_cc_library(
    name = "record_generator",
    srcs = [
        "record_generator.cc",
    ],
    hdrs = [
        "record_generator.h",
    ],
    implementation_deps = [
        "//src/v/container:chunked_hash_map",
        "//src/v/schema:registry",
        "//src/v/utils:absl_sstring_hash",
        "//src/v/utils:vint",
        "@avro",
        "@protobuf",
    ],
    include_prefix = "datalake/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/pandaproxy",
        "//src/v/serde/avro/tests:data_generator",
        "//src/v/serde/protobuf/tests:data_generator",
        "//src/v/storage:record_batch_builder",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "test_data_writer",
    hdrs = [
        "test_data_writer.h",
    ],
    include_prefix = "datalake/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/datalake:serde_parquet_writer",
        "//src/v/datalake:writer",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:values",
        "//src/v/utils:null_output_stream",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "test_utils",
    srcs = [
        "test_utils.cc",
    ],
    hdrs = [
        "test_utils.h",
    ],
    implementation_deps = [
        "//src/v/datalake:record_translator",
        "//src/v/datalake:table_definition",
        "//src/v/datalake:table_id_provider",
    ],
    include_prefix = "datalake/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/datalake:catalog_schema_manager",
        "//src/v/datalake:record_schema_resolver",
        "//src/v/datalake:table_creator",
    ],
)

redpanda_cc_gtest(
    name = "catalog_schema_manager_test",
    timeout = "short",
    srcs = [
        "catalog_schema_manager_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/datalake:catalog_schema_manager",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:field_collecting_visitor",
        "//src/v/iceberg:filesystem_catalog",
        "//src/v/iceberg:table_identifier",
        "//src/v/iceberg/tests:test_schemas",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "partition_spec_parser_test",
    timeout = "short",
    srcs = [
        "partition_spec_parser_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/datalake:partition_spec_parser",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "partitioning_writer_test",
    timeout = "short",
    srcs = [
        "partitioning_writer_test.cc",
    ],
    deps = [
        ":test_data_writer",
        ":test_utils",
        "//src/v/bytes",
        "//src/v/datalake:partitioning_writer",
        "//src/v/datalake:table_definition",
        "//src/v/iceberg/tests:test_schemas",
        "//src/v/iceberg/tests:value_generator",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "record_schema_resolver_test",
    timeout = "short",
    srcs = [
        "record_schema_resolver_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/config",
        "//src/v/datalake:logger",
        "//src/v/datalake:record_schema_resolver",
        "//src/v/iceberg:datatypes",
        "//src/v/pandaproxy",
        "//src/v/schema:registry",
        "//src/v/schema/tests:fake_registry",
        "//src/v/test_utils:gtest",
        "//src/v/utils:vint",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "schema_registry_test",
    timeout = "short",
    srcs = [
        "schema_registry_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/datalake:schema_registry",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "//src/v/utils:vint",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "local_parquet_file_writer_test",
    timeout = "short",
    srcs = [
        "local_parquet_file_writer_test.cc",
    ],
    deps = [
        ":test_data_writer",
        "//src/v/datalake:local_parquet_file_writer",
        "//src/v/datalake/tests:test_data",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg/tests:value_generator",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:seastar_boost",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "serde_parquet_writer_test",
    timeout = "short",
    srcs = [
        "serde_parquet_writer_test.cc",
    ],
    deps = [
        ":test_data_writer",
        "//src/v/bytes:iostream",
        "//src/v/datalake:serde_parquet_writer",
        "//src/v/datalake/tests:test_data",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg/tests:value_generator",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:seastar_boost",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "partition_key_path_test",
    timeout = "short",
    srcs = [
        "partition_key_path_test.cc",
    ],
    deps = [
        ":test_data",
        "//src/v/datalake:partition_key_path",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)
