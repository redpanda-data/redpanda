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
        "//src/v/cluster:state_machine_registry",
        "//src/v/datalake:logger",
        "//src/v/raft",
        "//src/v/serde",
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
    include_prefix = "datalake/translation",
    visibility = ["//visibility:public"],
    deps = [
        ":model",
        ":stm",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/datalake:cloud_data_io",
        "//src/v/datalake:local_parquet_file_writer",
        "//src/v/datalake:logger",
        "//src/v/datalake:record_multiplexer",
        "//src/v/datalake:record_schema_resolver",
        "//src/v/datalake:record_translator",
        "//src/v/datalake:serde_parquet_writer",
        "//src/v/datalake:table_creator",
        "//src/v/datalake:translation_task",
        "//src/v/datalake:types",
        "//src/v/datalake:writer",
        "//src/v/datalake/coordinator:frontend",
        "//src/v/datalake/coordinator:translated_offset_range",
        "//src/v/features",
        "//src/v/kafka/data:partition_proxy",
        "//src/v/kafka/utils:txn_reader",
        "//src/v/model",
        "//src/v/random:time_jitter",
        "//src/v/resource_mgmt:io_priority",
        "//src/v/ssx:future_util",
        "//src/v/ssx:semaphore",
        "//src/v/utils:lazy_abort_source",
        "//src/v/utils:prefix_logger",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)
