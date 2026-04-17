load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "catalog_and_registry_fixture",
    hdrs = ["catalog_and_registry_fixture.h"],
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
        "//src/v/iceberg/conversion/json_schema:frontend",
        "//src/v/iceberg/conversion/json_schema:ir",
        "//src/v/json",
        "//src/v/pandaproxy/schema_registry:registry",
        "//src/v/schema:registry",
        "//src/v/utils:absl_sstring_hash",
        "//src/v/utils:vint",
        "@avro",
        "@protobuf",
        "@rapidjson",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "//src/v/iceberg/conversion/json_schema/tests:data_generator",
        "//src/v/model",
        "//src/v/pandaproxy/schema_registry:server",
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
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/container:chunked_hash_map",
        "//src/v/datalake:serde_parquet_writer",
        "//src/v/datalake:writer",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:values",
        "//src/v/utils:null_output_stream",
        "//src/v/utils:uuid",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "record_batch_factory",
    hdrs = ["record_batch_factory.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/model",
        "//src/v/pandaproxy/schema_registry:server",
        "//src/v/storage:record_batch_builder",
        "//src/v/utils:vint",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "iceberg_table_reader",
    srcs = ["iceberg_table_reader.cc"],
    hdrs = ["iceberg_table_reader.h"],
    implementation_deps = [
        "//src/v/serde/parquet:assembler",
        "//src/v/serde/parquet:file_io",
        "//src/v/serde/parquet:reader",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/iceberg:manifest_entry",
        "//src/v/iceberg:manifest_io",
        "//src/v/iceberg:parquet_reader",
        "//src/v/iceberg:table_metadata",
        "//src/v/serde/parquet:value",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "e2e_test_utils",
    srcs = ["e2e_test_utils.cc"],
    hdrs = ["e2e_test_utils.h"],
    implementation_deps = [
        "//src/v/datalake/coordinator:data_file",
        "//src/v/iceberg:values_bytes",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/datalake:partitioning_writer",
        "//src/v/datalake:record_multiplexer",
        "//src/v/datalake/coordinator:translated_offset_range",
        "//src/v/iceberg:manifest_io",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "datalake_pipeline_test_fixture",
    srcs = ["datalake_pipeline_test_fixture.cc"],
    hdrs = ["datalake_pipeline_test_fixture.h"],
    implementation_deps = [
        ":e2e_test_utils",
        ":iceberg_table_reader",
        "//src/v/cloud_io:provider",
        "//src/v/config",
        "//src/v/datalake:local_parquet_file_writer",
        "//src/v/datalake:location",
        "//src/v/datalake:table_definition",
        "//src/v/iceberg:table_identifier",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":catalog_and_registry_fixture",
        ":test_data_writer",
        ":test_utils",
        "//src/v/datalake:catalog_schema_manager",
        "//src/v/datalake:cdc_key_value_translator",
        "//src/v/datalake:record_multiplexer",
        "//src/v/datalake:record_schema_resolver",
        "//src/v/datalake:record_translator",
        "//src/v/datalake/coordinator:iceberg_file_committer",
        "//src/v/datalake/coordinator:state",
        "//src/v/datalake/translation:translation_probe",
        "//src/v/features",
        "//src/v/iceberg:manifest_io",
        "//src/v/serde/parquet:value",
        "//src/v/storage",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "e2e_pipeline_test",
    timeout = "short",
    srcs = ["e2e_pipeline_test.cc"],
    cpu = 1,
    deps = [
        ":datalake_pipeline_test_fixture",
        ":record_batch_factory",
        "//src/v/model",
        "//src/v/serde/parquet:value",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
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
        "//src/v/features",
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
    name = "record_multiplexer_test",
    timeout = "short",
    srcs = [
        "record_multiplexer_test.cc",
    ],
    cpu = 1,
    deps = [
        ":catalog_and_registry_fixture",
        ":record_generator",
        ":test_data_writer",
        ":test_utils",
        "//src/v/cloud_io:provider",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/datalake:catalog_schema_manager",
        "//src/v/datalake:location",
        "//src/v/datalake:record_multiplexer",
        "//src/v/datalake:record_schema_resolver",
        "//src/v/datalake:record_translator",
        "//src/v/datalake:table_definition",
        "//src/v/datalake:table_id_provider",
        "//src/v/datalake/translation:translation_probe",
        "//src/v/features",
        "//src/v/iceberg:filesystem_catalog",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/random:generators",
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
        "//src/v/config",
        "//src/v/datalake:logger",
        "//src/v/datalake:record_schema_resolver",
        "//src/v/iceberg:datatypes",
        "//src/v/metrics",
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/schema:registry",
        "//src/v/schema/tests:fake_registry",
        "//src/v/test_utils:gtest",
        "//src/v/utils:vint",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "debezium_translator_test",
    timeout = "short",
    srcs = [
        "debezium_translator_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/datalake:debezium_translator",
        "//src/v/datalake:record_schema_resolver",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:values",
        "//src/v/model",
        "//src/v/schema/tests:fake_registry",
        "//src/v/test_utils:gtest",
        "@avro",
        "@googletest//:gtest",
        "@seastar",
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
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "record_multiplexer_rpbench",
    timeout = "long",
    srcs = [
        "record_multiplexer_bench.cc",
    ],
    test_regex = ".*_linear_1_field_small",
    deps = [
        ":catalog_and_registry_fixture",
        ":record_generator",
        ":test_data_writer",
        ":test_utils",
        "//src/v/cloud_io:provider",
        "//src/v/container:chunked_vector",
        "//src/v/datalake:catalog_schema_manager",
        "//src/v/datalake:location",
        "//src/v/datalake:record_multiplexer",
        "//src/v/datalake:record_schema_resolver",
        "//src/v/datalake:record_translator",
        "//src/v/datalake:table_creator",
        "//src/v/features",
        "//src/v/model",
        "//src/v/model:batch_compression",
        "//src/v/serde/avro/tests:data_generator",
        "//src/v/serde/protobuf/tests:data_generator",
        "@seastar",
        "@seastar//:benchmark",
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

redpanda_cc_gtest(
    name = "translation_task_test",
    timeout = "short",
    srcs = [
        "translation_task_test.cc",
    ],
    cpu = 1,
    deps = [
        ":test_data_writer",
        ":test_utils",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/datalake:catalog_schema_manager",
        "//src/v/datalake:cloud_data_io",
        "//src/v/datalake:local_parquet_file_writer",
        "//src/v/datalake:location",
        "//src/v/datalake:record_schema_resolver",
        "//src/v/datalake:record_translator",
        "//src/v/datalake:table_creator",
        "//src/v/datalake:translation_task",
        "//src/v/datalake/translation:translation_probe",
        "//src/v/features",
        "//src/v/iceberg:uri",
        "//src/v/model",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@boost//:range",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "gtest_record_multiplexer_test",
    timeout = "short",
    srcs = [
        "gtest_record_multiplexer_test.cc",
    ],
    cpu = 1,
    deps = [
        ":catalog_and_registry_fixture",
        ":record_generator",
        ":test_data_writer",
        ":test_utils",
        "//src/v/cloud_io:provider",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/datalake:base_types",
        "//src/v/datalake:catalog_schema_manager",
        "//src/v/datalake:cloud_data_io",
        "//src/v/datalake:local_parquet_file_writer",
        "//src/v/datalake:location",
        "//src/v/datalake:record_multiplexer",
        "//src/v/datalake:record_schema_resolver",
        "//src/v/datalake:record_translator",
        "//src/v/datalake:serde_parquet_writer",
        "//src/v/datalake:table_creator",
        "//src/v/datalake:translation_task",
        "//src/v/datalake/translation:translation_probe",
        "//src/v/features",
        "//src/v/iceberg:filesystem_catalog",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "translator_fixture",
    hdrs = ["fixture.h"],
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/cluster/archival/tests:archival_service_fixture",
        "//src/v/datalake/coordinator:frontend",
        "//src/v/datalake/coordinator:stm",
    ],
)

redpanda_cc_btest(
    name = "translator_fixture_test",
    timeout = "moderate",
    srcs = [
        "translator_test.cc",
    ],
    cpu = 1,
    deps = [
        ":translator_fixture",
        "//src/v/kafka/client",
        "//src/v/kafka/client/test:utils",
        "//src/v/model/tests:random",
        "//src/v/test_utils:random",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "credential_manager_test",
    timeout = "short",
    srcs = [
        "credential_manager_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_roles",
        "//src/v/cloud_roles:types",
        "//src/v/config",
        "//src/v/datalake:credential_manager",
        "//src/v/hashing:secure",
        "//src/v/http",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@boost//:beast",
        "@googletest//:gtest",
    ],
)
