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
        "//src/v/features",
        "//src/v/iceberg:compatibility",
        "//src/v/iceberg:compatibility_types",
        "//src/v/iceberg:partition",
        "//src/v/iceberg:schema",
        "//src/v/iceberg:transaction",
    ],
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/iceberg:catalog",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:table_identifier",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "schema_identifier",
    hdrs = [
        "schema_identifier.h",
    ],
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/pandaproxy/schema_registry:types",
        "//src/v/serde",
        "@boost//:container_hash",
    ],
)

redpanda_cc_library(
    name = "logger",
    hdrs = [
        "logger.h",
    ],
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
    implementation_deps = [
        "//src/v/net:types",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":logger",
        "//src/v/base",
        "//src/v/cloud_roles",
        "//src/v/cloud_roles:auth_refresh_bg_op",
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
        "//src/v/cluster/utils:partition_change_notifier_api",
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
        "//src/v/pandaproxy/schema_registry:types",
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
        "//src/v/container:chunked_vector",
        "//src/v/iceberg:transform",
        "//src/v/utils:fixed_string",
        "//src/v/utils:variant",
        "@fmt",
        "@seastar",
    ],
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
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/iceberg:schema",
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
    visibility = ["//visibility:public"],
    deps = [
        ":base_types",
        ":logger",
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
        "//src/v/container:chunked_vector",
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
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/pandaproxy/schema_registry:types",
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
        ":logger",
        ":record_schema_resolver",
        ":record_translator",
        ":table_creator",
        ":table_id_provider",
        "//src/v/base",
        "//src/v/features",
        "//src/v/iceberg/conversion:conversion_outcome",
        "//src/v/model:batch_compression",
        "//src/v/utils:lazy_abort_source",
        "//src/v/utils:prefix_logger",
    ],
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
        ":logger",
        ":schema_registry",
        "//src/v/iceberg/conversion:conversion_outcome",
        "//src/v/iceberg/conversion:ir_json",
        "//src/v/iceberg/conversion:schema_avro",
        "//src/v/iceberg/conversion:schema_json",
        "//src/v/iceberg/conversion:schema_protobuf",
        "//src/v/iceberg/conversion/json_schema:frontend",
        "//src/v/metrics",
        "//src/v/pandaproxy/schema_registry:registry",
        "//src/v/schema:registry",
        "@avro",
        "@protobuf",
    ],
    visibility = [":__subpackages__"],
    deps = [
        ":schema_identifier",
        "//src/v/base",
        "//src/v/config",
        "//src/v/iceberg:datatypes",
        "//src/v/json",
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
        ":logger",
        ":schema_registry",
        ":table_definition",
        "//src/v/iceberg:avro_utils",
        "//src/v/iceberg:compatibility_utils",
        "//src/v/iceberg:values",
        "//src/v/iceberg/conversion:conversion_outcome",
        "//src/v/iceberg/conversion:values_avro",
        "//src/v/iceberg/conversion:values_json",
        "//src/v/iceberg/conversion:values_protobuf",
        "//src/v/metrics",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@avro",
        "@protobuf",
        "@seastar",
    ],
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
    name = "local_parquet_file_writer",
    srcs = [
        "local_parquet_file_writer.cc",
    ],
    hdrs = [
        "local_parquet_file_writer.h",
    ],
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
        "//src/v/iceberg:values",
        "//src/v/iceberg/conversion:schema_parquet",
        "//src/v/iceberg/conversion:values_parquet",
        "//src/v/version",
    ],
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
    visibility = [":__subpackages__"],
    deps = [
        ":schema_identifier",
    ],
)

redpanda_cc_library(
    name = "table_id_provider",
    srcs = [
        "table_id_provider.cc",
    ],
    hdrs = [
        "table_id_provider.h",
    ],
    implementation_deps = [
        "//src/v/config",
        "@boost//:algorithm",
    ],
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
    visibility = ["//visibility:public"],
    deps = [
        ":logger",
        ":types",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/datalake/coordinator:frontend",
        "//src/v/kafka/server:datalake_usage_api",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)
