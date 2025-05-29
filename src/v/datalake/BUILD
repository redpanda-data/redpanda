load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "catalog_schema_manager",
    srcs = [
        "catalog_schema_manager.cc",
    ],
    hdrs = [
        "catalog_schema_manager.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/base",
        "//src/v/iceberg:compatibility",
        "//src/v/iceberg:field_collecting_visitor",
        "//src/v/iceberg:partition",
        "//src/v/iceberg:schema",
        "//src/v/iceberg:transaction",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/iceberg:catalog",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:table_identifier",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "conversion_outcome",
    srcs = [
    ],
    hdrs = [
        "conversion_outcome.h",
    ],
    include_prefix = "datalake",
    deps = [
        "//src/v/iceberg:values",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "protobuf_utils",
    srcs = [
        "protobuf_utils.cc",
    ],
    hdrs = [
        "protobuf_utils.h",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        "@protobuf",
    ],
)

redpanda_cc_library(
    name = "schema_identifier",
    hdrs = [
        "schema_identifier.h",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/pandaproxy",
        "//src/v/serde",
        "@boost//:container_hash",
    ],
)

redpanda_cc_library(
    name = "schema_protobuf",
    srcs = [
        "schema_protobuf.cc",
    ],
    hdrs = [
        "schema_protobuf.h",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        ":conversion_outcome",
        ":protobuf_utils",
        "//src/v/iceberg:datatypes",
        "@fmt",
        "@protobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "logger",
    hdrs = [
        "logger.h",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "partitioning_writer",
    srcs = [
        "partitioning_writer.cc",
    ],
    hdrs = [
        "partitioning_writer.h",
    ],
    implementation_deps = [
        ":logger",
        "//src/v/base",
        "//src/v/iceberg:struct_accessor",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        ":partition_key_path",
        ":writer",
        "//src/v/container:chunked_hash_map",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:partition_key",
        "//src/v/iceberg:schema",
        "//src/v/iceberg:values",
    ],
)

redpanda_cc_library(
    name = "types",
    hdrs = [
        "errors.h",
        "fwd.h",
    ],
    include_prefix = "datalake",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "writer",
    srcs = [
        "data_writer_interface.cc",
    ],
    hdrs = [
        "data_writer_interface.h",
    ],
    implementation_deps = [
        "@fmt",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        ":base_types",
        "//src/v/base",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:values",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "credential_manager",
    srcs = [
        "credential_manager.cc",
    ],
    hdrs = [
        "credential_manager.h",
    ],
    include_prefix = "datalake",
    visibility = ["//visibility:public"],
    deps = [
        ":logger",
        "//src/v/base",
        "//src/v/cloud_io:auth_refresh_bg_op",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_roles",
        "//src/v/cloud_roles:types",
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/hashing:secure",
        "//src/v/net",
        "@boost//:beast",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "manager",
    srcs = [
        "datalake_manager.cc",
    ],
    hdrs = [
        "datalake_manager.h",
    ],
    include_prefix = "datalake",
    visibility = ["//visibility:public"],
    deps = [
        ":backlog_controller",
        ":catalog_schema_manager",
        ":cloud_data_io",
        ":location",
        ":logger",
        ":record_translator",
        ":types",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/datalake:record_schema_resolver",
        "//src/v/datalake/coordinator:catalog_factory",
        "//src/v/datalake/coordinator:frontend",
        "//src/v/datalake/translation:partition_translator",
        "//src/v/datalake/translation:scheduler",
        "//src/v/datalake/translation:translation_probe",
        "//src/v/features",
        "//src/v/model",
        "//src/v/pandaproxy",
        "//src/v/raft",
        "//src/v/schema:registry",
        "//src/v/serde",
        "//src/v/ssx:semaphore",
        "//src/v/ssx:work_queue",
        "//src/v/utils:directory_walker",
        "//src/v/utils:human",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "partition_spec_parser",
    srcs = [
        "partition_spec_parser.cc",
    ],
    hdrs = [
        "partition_spec_parser.h",
    ],
    implementation_deps = [
        "//src/v/container:fragmented_vector",
        "//src/v/iceberg:transform",
        "//src/v/utils:fixed_string",
        "//src/v/utils:variant",
        "@fmt",
        "@seastar",
    ],
    include_prefix = "datalake",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/iceberg:unresolved_partition_spec",
    ],
)

redpanda_cc_library(
    name = "translation_task",
    srcs = [
        "translation_task.cc",
    ],
    hdrs = [
        "translation_task.h",
    ],
    include_prefix = "datalake",
    visibility = ["//visibility:public"],
    deps = [
        ":cloud_data_io",
        ":location",
        ":logger",
        ":record_multiplexer",
        ":types",
        ":writer",
        "//src/v/base",
        "//src/v/datalake/coordinator:translated_offset_range",
        "//src/v/datalake/translation:translation_probe",
        "//src/v/iceberg:values_bytes",
        "//src/v/model",
        "//src/v/utils:lazy_abort_source",
        "//src/v/utils:retry_chain_node",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "table_definition",
    srcs = [
        "table_definition.cc",
    ],
    hdrs = [
        "table_definition.h",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/iceberg:schema",
    ],
)

redpanda_cc_library(
    name = "values_protobuf",
    srcs = [
        "values_protobuf.cc",
    ],
    hdrs = [
        "values_protobuf.h",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        ":conversion_outcome",
        ":protobuf_utils",
        "//src/v/bytes:iobuf",
        "//src/v/container:fragmented_vector",
        "//src/v/iceberg:values",
        "//src/v/serde/protobuf",
        "//src/v/ssx:future_util",
        "@abseil-cpp//absl/time",
        "@fmt",
        "@protobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "base_types",
    srcs = [
        "base_types.cc",
    ],
    hdrs = [
        "base_types.h",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/utils:named_type",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "cloud_data_io",
    srcs = [
        "cloud_data_io.cc",
    ],
    hdrs = [
        "cloud_data_io.h",
    ],
    include_prefix = "datalake",
    visibility = ["//visibility:public"],
    deps = [
        ":base_types",
        ":logger",
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/utils:lazy_abort_source",
        "//src/v/utils:named_type",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:stream_provider",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "schema_registry",
    srcs = ["schema_registry.cc"],
    hdrs = ["schema_registry.h"],
    implementation_deps = [
        ":logger",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/pandaproxy",
    ],
)

redpanda_cc_library(
    name = "record_multiplexer",
    srcs = [
        "record_multiplexer.cc",
    ],
    hdrs = [
        "record_multiplexer.h",
    ],
    implementation_deps = [
        ":catalog_schema_manager",
        ":conversion_outcome",
        ":logger",
        ":record_schema_resolver",
        ":record_translator",
        ":table_creator",
        ":table_id_provider",
        "//src/v/base",
        "//src/v/storage:parser_utils",
        "//src/v/utils:lazy_abort_source",
        "//src/v/utils:prefix_logger",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        ":location",
        ":partitioning_writer",
        ":schema_identifier",
        ":types",
        ":writer",
        "//src/v/container:chunked_hash_map",
        "//src/v/datalake/translation:translation_probe",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "record_schema_resolver",
    srcs = [
        "record_schema_resolver.cc",
    ],
    hdrs = [
        "record_schema_resolver.h",
    ],
    implementation_deps = [
        ":conversion_outcome",
        ":logger",
        ":schema_avro",
        ":schema_protobuf",
        ":schema_registry",
        "//src/v/metrics",
        "//src/v/pandaproxy",
        "//src/v/schema:registry",
        "@avro",
        "@protobuf",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        ":schema_identifier",
        "//src/v/base",
        "//src/v/config",
        "//src/v/iceberg:datatypes",
        "//src/v/utils:chunked_kv_cache",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "record_translator",
    srcs = [
        "record_translator.cc",
    ],
    hdrs = [
        "record_translator.h",
    ],
    implementation_deps = [
        ":conversion_outcome",
        ":logger",
        ":schema_registry",
        ":table_definition",
        ":values_avro",
        ":values_protobuf",
        "//src/v/iceberg:avro_utils",
        "//src/v/iceberg:compatibility_utils",
        "//src/v/iceberg:values",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@avro",
        "@protobuf",
        "@seastar",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        ":record_schema_resolver",
        ":schema_identifier",
        "//src/v/base",
        "//src/v/iceberg:datatypes",
        "//src/v/model",
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
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        ":conversion_outcome",
        "//src/v/iceberg:datatypes",
        "@avro",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "values_avro",
    srcs = ["values_avro.cc"],
    hdrs = ["values_avro.h"],
    implementation_deps = [
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/iceberg:avro_decimal",
        "//src/v/serde/avro",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        ":conversion_outcome",
        "@avro",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "schema_parquet",
    srcs = [
        "schema_parquet.cc",
    ],
    hdrs = [
        "schema_parquet.h",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/iceberg:datatypes",
        "//src/v/serde/parquet:schema",
    ],
)

redpanda_cc_library(
    name = "values_parquet",
    srcs = [
        "values_parquet.cc",
    ],
    hdrs = [
        "values_parquet.h",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        ":conversion_outcome",
        "//src/v/iceberg:values",
        "//src/v/serde/parquet:value",
    ],
)

redpanda_cc_library(
    name = "local_parquet_file_writer",
    srcs = [
        "local_parquet_file_writer.cc",
    ],
    hdrs = [
        "local_parquet_file_writer.h",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        ":logger",
        ":writer",
        "//src/v/base",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:values",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "serde_parquet_writer",
    srcs = [
        "serde_parquet_writer.cc",
    ],
    hdrs = [
        "serde_parquet_writer.h",
    ],
    implementation_deps = [
        ":logger",
        ":schema_parquet",
        ":values_parquet",
        "//src/v/iceberg:values",
        "//src/v/version",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        ":writer",
        "//src/v/base",
        "//src/v/iceberg:datatypes",
        "//src/v/serde/parquet:writer",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "table_creator",
    srcs = [
        "table_creator.cc",
    ],
    hdrs = [
        "table_creator.h",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        ":schema_identifier",
    ],
)

redpanda_cc_library(
    name = "table_id_provider",
    hdrs = [
        "table_id_provider.h",
    ],
    include_prefix = "datalake",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/iceberg:table_identifier",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "location",
    hdrs = [
        "location.h",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        ":base_types",
        "//src/v/iceberg:uri",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "backlog_controller",
    srcs = [
        "backlog_controller.cc",
    ],
    hdrs = [
        "backlog_controller.h",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        ":logger",
        "//src/v/base",
        "//src/v/config",
        "//src/v/metrics",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "partition_key_path",
    srcs = [
        "partition_key_path.cc",
    ],
    hdrs = [
        "partition_key_path.h",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        ":base_types",
        ":logger",
        "//src/v/base",
        "//src/v/bytes:iobuf_parser",
        "//src/v/http:utils",
        "//src/v/iceberg:partition_key",
        "//src/v/ssx:sformat",
        "//src/v/utils:base64",
        "@abseil-cpp//absl/strings",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "datalake_usage_aggregator",
    srcs = [
        "datalake_usage_aggregator.cc",
    ],
    hdrs = [
        "datalake_usage_aggregator.h",
    ],
    include_prefix = "datalake",
    visibility = ["//visibility:public"],
    deps = [
        ":logger",
        ":types",
        "//src/v/cluster",
        "//src/v/kafka/server:datalake_usage_api",
    ],
)
