load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "model",
    hdrs = [
        "types.h",
    ],
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
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
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
    name = "translator_deps",
    srcs = [
        "deps.cc",
    ],
    hdrs = [
        "deps.h",
    ],
    visibility = [":__subpackages__"],
    deps = [
        ":scheduler",
        ":stm",
        ":translation_probe",
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
        "//src/v/datalake:translation_task",
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
