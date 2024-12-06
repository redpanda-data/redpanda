load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("//bazel:build.bzl", "redpanda_cc_binary", "redpanda_cc_library")

redpanda_cc_library(
    name = "application",
    srcs = [
        "application.cc",
        "cli_parser.cc",
        "monitor_unsafe.cc",
    ],
    hdrs = [
        "application.h",
        "cli_parser.h",
        "monitor_unsafe.h",
    ],
    include_prefix = "redpanda",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics:app",
        "//src/v/cloud_topics/dl_stm:dl_stm_factory",
        "//src/v/cluster",
        "//src/v/cluster:cloud_metadata_offsets_recovery_router",
        "//src/v/cluster:cloud_metadata_offsets_recovery_service",
        "//src/v/cluster:cloud_metadata_offsets_upload_router",
        "//src/v/cluster:offsets_lookup",
        "//src/v/compression",
        "//src/v/config",
        "//src/v/crypto",
        #       "//src/v/datalake:manager",
        "//src/v/datalake:types",
        "//src/v/datalake:cloud_data_io",
        "//src/v/datalake:manager",
        "//src/v/datalake/coordinator:coordinator_manager",
        "//src/v/datalake/coordinator:frontend",
        "//src/v/datalake/coordinator:stm",
        "//src/v/datalake/coordinator:catalog_factory",
        "//src/v/datalake/translation:stm",
        "//src/v/debug_bundle",
        "//src/v/features",
        "//src/v/finjector",
        "//src/v/kafka/client",
        "//src/v/kafka/server",
        "//src/v/kafka/server:app",
        "//src/v/kafka/server:qdc_monitor_config",
        "//src/v/metrics",
        "//src/v/migrations",
        "//src/v/model",
        "//src/v/net",
        "//src/v/pandaproxy",
        "//src/v/raft",
        "//src/v/redpanda/admin",
        "//src/v/resource_mgmt:cpu_profiler",
        "//src/v/resource_mgmt:cpu_scheduling",
        "//src/v/resource_mgmt:io_priority",
        "//src/v/resource_mgmt:memory_groups",
        "//src/v/resource_mgmt:memory_sampling",
        "//src/v/resource_mgmt:scheduling_groups_probe",
        "//src/v/resource_mgmt:smp_groups",
        "//src/v/resource_mgmt:storage",
        "//src/v/rpc",
        "//src/v/security",
        "//src/v/security/audit",
        "//src/v/ssx:abort_source",
        "//src/v/ssx:thread_worker",
        "//src/v/storage",
        "//src/v/storage:chunk_cache",
        "//src/v/strings:utf8",
        "//src/v/syschecks",
        "//src/v/transform",
        "//src/v/transform:fwd",
        "//src/v/transform/rpc",
        "//src/v/transform/stm",
        "//src/v/utils:file_io",
        "//src/v/utils:human",
        "//src/v/utils:named_type",
        "//src/v/utils:stop_signal",
        "//src/v/utils:uuid",
        "//src/v/version",
        "//src/v/wasm:api",
        "//src/v/wasm:cache",
        "//src/v/wasm:impl",
        "@abseil-cpp//absl/log:globals",
        "@boost//:program_options",
        "@fmt",
        "@protobuf",
        "@protobuf//:protobuf_lite",
        "@seastar",
    ],
)

# Enable or disable (thin) LTO for the redpanda binary (only)
bool_flag(
    name = "lto",
    build_setting_default = False,
)

config_setting(
    name = "use_lto",
    flag_values = {
        ":lto": "true",
    },
    visibility = ["//visibility:public"],
)

redpanda_cc_binary(
    name = "redpanda",
    srcs = [
        "main.cc",
    ],
    linkopts =
        select({
            ":use_lto": [
                "-flto=thin",
                "-ffat-lto-objects",
            ],
            "//conditions:default": [
            ],
        }),
    visibility = ["//visibility:public"],
    deps = [
        ":application",
        "//src/v/syschecks",
    ],
)
