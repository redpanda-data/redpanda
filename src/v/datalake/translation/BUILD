load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "model",
    hdrs = [
        "types.h",
    ],
    include_prefix = "datalake/translation",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/model",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "utils",
    srcs = [
        "utils.cc",
    ],
    hdrs = [
        "utils.h",
    ],
    include_prefix = "datalake/translation",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/model",
        "//src/v/serde",
        "//src/v/storage",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "scheduler",
    srcs = [
        "scheduling.cc",
        "scheduling_policies.cc",
    ],
    hdrs = [
        "scheduling.h",
        "scheduling_policies.h",
    ],
    include_prefix = "datalake/translation",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:fragmented_vector",
        "//src/v/container:intrusive",
        "//src/v/datalake:logger",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/ssx:future_util",
        "//src/v/ssx:semaphore",
        "//src/v/utils:human",
        "//src/v/utils:to_string",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "stm",
    srcs = [
        "state_machine.cc",
    ],
    hdrs = [
        "state_machine.h",
    ],
    include_prefix = "datalake/translation",
    visibility = ["//visibility:public"],
    deps = [
        ":model",
        ":utils",
        "//src/v/base",
        "//src/v/cluster:state_machine_registry",
        "//src/v/datalake:logger",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/serde",
        "@seastar",
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
        "//src/v/base",
        "//src/v/datalake:catalog_schema_manager",
        "//src/v/iceberg/conversion:conversion_outcome",
        "//src/v/datalake:logger",
        "//src/v/datalake:record_schema_resolver",
        "//src/v/datalake:record_translator",
        "//src/v/datalake:table_creator",
        "//src/v/datalake:table_id_provider",
        "//src/v/storage:parser_utils",
        "//src/v/utils:lazy_abort_source",
        "//src/v/utils:prefix_logger",
    ],
    include_prefix = "datalake/translation",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/container:chunked_hash_map",
        "//src/v/datalake:location",
        "//src/v/datalake:partitioning_writer",
        "//src/v/datalake:schema_identifier",
        "//src/v/datalake:types",
        "//src/v/datalake:writer",
        "//src/v/datalake/translation:translation_probe",
        "//src/v/model",
        "@seastar",
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
    include_prefix = "datalake/translation",
    visibility = ["//visibility:public"],
    deps = [
        ":record_multiplexer",
        "//src/v/base",
        "//src/v/datalake:cloud_data_io",
        "//src/v/datalake:location",
        "//src/v/datalake:logger",
        "//src/v/datalake:types",
        "//src/v/datalake:writer",
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
    name = "translator_deps",
    srcs = [
        "deps.cc",
    ],
    hdrs = [
        "deps.h",
    ],
    include_prefix = "datalake/translation",
    visibility = [":__subpackages__"],
    deps = [
        ":scheduler",
        ":stm",
        ":translation_probe",
        ":translation_task",
        ":utils",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/datalake:local_parquet_file_writer",
        "//src/v/datalake:location",
        "//src/v/datalake:logger",
        "//src/v/datalake:record_schema_resolver",
        "//src/v/datalake:record_translator",
        "//src/v/datalake:serde_parquet_writer",
        "//src/v/datalake:table_creator",
        "//src/v/datalake:types",
        "//src/v/datalake:writer",
        "//src/v/datalake/coordinator:frontend",
        "//src/v/datalake/coordinator:model",
        "//src/v/features",
        "//src/v/kafka/data:partition_proxy",
        "//src/v/kafka/utils:txn_reader",
        "//src/v/model",
        "//src/v/utils:human",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "partition_translator",
    srcs = [
        "partition_translator.cc",
    ],
    hdrs = [
        "partition_translator.h",
    ],
    implementation_deps = [
        "//src/v/ssx:watchdog",
    ],
    include_prefix = "datalake/translation",
    visibility = ["//visibility:public"],
    deps = [
        ":model",
        ":scheduler",
        ":translation_probe",
        ":translator_deps",
        ":utils",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/datalake:logger",
        "//src/v/features",
        "//src/v/model",
        "//src/v/random:time_jitter",
        "//src/v/resource_mgmt:io_priority",
        "//src/v/ssx:future_util",
        "//src/v/ssx:semaphore",
        "//src/v/utils:prefix_logger",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:to_string",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "translation_probe",
    srcs = [
        "translation_probe.cc",
    ],
    hdrs = [
        "translation_probe.h",
    ],
    include_prefix = "datalake/translation",
    visibility = [
        "//src/v/datalake:__subpackages__",
    ],
    deps = [
        "//src/v/config",
        "//src/v/metrics",
        "//src/v/model",
        "@seastar",
    ],
)
