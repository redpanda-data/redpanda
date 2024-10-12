load("//bazel:build.bzl", "redpanda_cc_library")
load("//src/v/rpc:compiler.bzl", "redpanda_cc_rpc_library")

package(default_visibility = ["//src/v/datalake/coordinator:__subpackages__"])

redpanda_cc_rpc_library(
    name = "generated_datalake_coordinator_rpc",
    src = "rpc.json",
    include_prefix = "datalake/coordinator",
)

redpanda_cc_library(
    name = "coordinator",
    srcs = [
        "coordinator.cc",
    ],
    hdrs = [
        "coordinator.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/datalake:logger",
        "//src/v/storage:record_batch_builder",
    ],
    include_prefix = "datalake/coordinator",
    deps = [
        ":state_update",
        ":stm",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "coordinator_manager",
    srcs = [
        "coordinator_manager.cc",
    ],
    hdrs = [
        "coordinator_manager.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/datalake:logger",
        "//src/v/storage:record_batch_builder",
    ],
    include_prefix = "datalake/coordinator",
    visibility = ["//visibility:public"],
    deps = [
        ":coordinator",
        ":state_update",
        ":stm",
        "//src/v/cluster",
        "//src/v/cluster:notification",
        "//src/v/model",
        "//src/v/raft",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "data_file",
    srcs = [
        "data_file.cc",
    ],
    hdrs = [
        "data_file.h",
    ],
    include_prefix = "datalake/coordinator",
    visibility = [
        ":__subpackages__",
        "//src/v/datalake:__pkg__",
    ],
    deps = [
        "//src/v/base",
        "//src/v/serde",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "file_committer",
    hdrs = [
        "file_committer.h",
    ],
    include_prefix = "datalake/coordinator",
    deps = [
        ":state_update",
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "iceberg_file_committer",
    srcs = [
        "iceberg_file_committer.cc",
    ],
    hdrs = [
        "iceberg_file_committer.h",
    ],
    implementation_deps = [
        ":state",
        "//src/v/base",
        "//src/v/datalake:logger",
        "//src/v/datalake:table_definition",
        "//src/v/iceberg:manifest_entry",
        "//src/v/iceberg:partition_key",
        "//src/v/iceberg:table_metadata",
        "//src/v/iceberg:transaction",
        "//src/v/iceberg:values",
    ],
    include_prefix = "datalake/coordinator",
    deps = [
        ":file_committer",
        ":state_update",
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "//src/v/iceberg:catalog",
        "//src/v/iceberg:manifest_io",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "model",
    hdrs = [
        "types.h",
    ],
    include_prefix = "datalake/coordinator",
    visibility = [":__subpackages__"],
    deps = [
        ":translated_offset_range",
        "//src/v/datalake:types",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:enum",
    ],
)

redpanda_cc_library(
    name = "state",
    srcs = [
        "state.cc",
    ],
    hdrs = [
        "state.h",
    ],
    include_prefix = "datalake/coordinator",
    deps = [
        ":translated_offset_range",
        "//src/v/container:chunked_hash_map",
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
    include_prefix = "datalake/coordinator",
    visibility = ["//visibility:public"],
    deps = [
        ":model",
        ":state",
        ":state_update",
        "//src/v/cluster",
        "//src/v/datalake:logger",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/serde",
        "//src/v/serde:enum",
        "//src/v/ssx:future_util",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "state_update",
    srcs = [
        "state_update.cc",
    ],
    hdrs = [
        "state_update.h",
    ],
    include_prefix = "datalake/coordinator",
    deps = [
        ":state",
        ":translated_offset_range",
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/utils:named_type",
    ],
)

redpanda_cc_library(
    name = "translated_offset_range",
    srcs = [
        "translated_offset_range.cc",
    ],
    hdrs = [
        "translated_offset_range.h",
    ],
    include_prefix = "datalake/coordinator",
    deps = [
        ":data_file",
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/serde",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "frontend",
    srcs = [
        "frontend.cc",
        "service.cc",
    ],
    hdrs = [
        "frontend.h",
        "service.h",
    ],
    include_prefix = "datalake/coordinator",
    visibility = ["//visibility:public"],
    deps = [
        ":coordinator",
        ":coordinator_manager",
        ":generated_datalake_coordinator_rpc",
        ":model",
        ":stm",
        ":translated_offset_range",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/container:fragmented_vector",
        "//src/v/datalake:logger",
        "//src/v/datalake:types",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/rpc",
        "@fmt",
        "@seastar",
    ],
)
