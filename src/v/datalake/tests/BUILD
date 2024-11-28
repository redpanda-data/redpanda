load("@rules_proto//proto:defs.bzl", "proto_library")
load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

proto_library(
    name = "person_proto",
    srcs = ["testdata/person.proto"],
)

proto_library(
    name = "complex_proto",
    srcs = ["testdata/complex.proto"],
)

proto_library(
    name = "test_messages_proto",
    srcs = ["testdata/iceberg_ready_test_messages_edition2023.proto"],
)

proto_library(
    name = "not_supported_proto",
    srcs = ["testdata/not_supported.proto"],
)

proto_library(
    name = "proto2_proto",
    srcs = ["testdata/proto2.proto"],
)

cc_proto_library(
    name = "person_cc_proto",
    deps = [":person_proto"],
)

cc_proto_library(
    name = "complex_cc_proto",
    deps = [":complex_proto"],
)

cc_proto_library(
    name = "test_messages_cc_proto",
    deps = [":test_messages_proto"],
)

cc_proto_library(
    name = "not_supported_cc_proto",
    deps = [":not_supported_proto"],
)

cc_proto_library(
    name = "proto2_cc_proto",
    deps = [":proto2_proto"],
)

redpanda_test_cc_library(
    name = "catalog_and_registry_fixture",
    hdrs = ["catalog_and_registry_fixture.h"],
    include_prefix = "datalake/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage/tests:s3_imposter_gtest",
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

#TODO: this wrapper can be removed when we completely remove cmake tests support
redpanda_test_cc_library(
    name = "proto_definitions_wrapper",
    hdrs = ["proto_definitions.h"],
    defines = ["BAZEL_TEST=1"],
    include_prefix = "datalake/tests",
    deps = [
        ":complex_cc_proto",
        ":not_supported_cc_proto",
        ":person_cc_proto",
        ":proto2_cc_proto",
        ":test_messages_cc_proto",
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
        "//src/v/serde/avro/tests:data_generator",
        "@avro",
    ],
    include_prefix = "datalake/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/pandaproxy",
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
    deps = [
        "//src/v/datalake:writer",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:values",
        "@seastar",
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
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage/tests:s3_imposter_gtest",
        "//src/v/datalake:catalog_schema_manager",
        "//src/v/iceberg:field_collecting_visitor",
        "//src/v/iceberg:filesystem_catalog",
        "//src/v/iceberg:table_identifier",
        "//src/v/iceberg/tests:test_schemas",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "datalake_protobuf_test",
    timeout = "short",
    srcs = [
        "datalake_protobuf_tests.cc",
    ],
    deps = [
        ":complex_cc_proto",
        ":person_cc_proto",
        ":proto2_cc_proto",
        ":proto_definitions_wrapper",
        ":test_messages_cc_proto",
        "//src/v/datalake:schema_protobuf",
        "//src/v/datalake:values_protobuf",
        "//src/v/iceberg:datatypes",
        "//src/v/serde/protobuf",
        "//src/v/ssx:future_util",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@protobuf",
        "@protobuf//src/google/protobuf/compiler:importer",
        "@protobuf//src/google/protobuf/io:tokenizer",
        "@seastar",
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
    name = "record_multiplexer_test",
    timeout = "short",
    srcs = [
        "record_multiplexer_test.cc",
    ],
    cpu = 1,
    tags = ["exclusive"],
    deps = [
        ":catalog_and_registry_fixture",
        ":record_generator",
        ":test_data_writer",
        "//src/v/datalake:catalog_schema_manager",
        "//src/v/datalake:record_multiplexer",
        "//src/v/datalake:record_schema_resolver",
        "//src/v/datalake:record_translator",
        "//src/v/datalake:table_creator",
        "//src/v/datalake:table_definition",
        "//src/v/iceberg:filesystem_catalog",
        "//src/v/model",
        "//src/v/pandaproxy",
        "//src/v/schema:registry",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@avro",
        "@googletest//:gtest",
        "@seastar",
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
    name = "datalake_avro_tests",
    timeout = "short",
    srcs = [
        "datalake_avro_tests.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/datalake:schema_avro",
        "//src/v/datalake:values_avro",
        "//src/v/iceberg:avro_decimal",
        "//src/v/iceberg:datatypes",
        "//src/v/serde/avro/tests:data_generator",
        "//src/v/test_utils:gtest",
        "@avro",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "datalake_parquet_tests",
    timeout = "short",
    srcs = [
        "datalake_parquet_tests.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/datalake:schema_parquet",
        "//src/v/datalake:values_parquet",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg/tests:value_generator",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "local_parquet_file_writer_test",
    timeout = "short",
    srcs = [
        "local_parquet_file_writer_test.cc",
    ],
    deps = [
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
