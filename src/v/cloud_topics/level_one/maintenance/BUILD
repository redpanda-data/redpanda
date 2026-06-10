load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/level_one:__subpackages__"])

redpanda_cc_library(
    name = "logger",
    hdrs = [
        "logger.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "l1_object_sink",
    srcs = [
        "l1_object_sink.cc",
    ],
    hdrs = [
        "l1_object_sink.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        ":logger",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:offset_interval_set",
        "//src/v/cloud_topics/level_one/metastore:retry",
        "//src/v/compaction:reducer",
        "//src/v/config",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/utils:prefix_logger",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "worker",
    srcs = [
        "worker.cc",
        "worker_manager.cc",
    ],
    hdrs = [
        "worker.h",
        "worker_manager.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        ":logger",
        ":meta",
        ":scheduler_probe",
        ":worker_probe",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:file_io",
        "//src/v/cloud_topics/level_one/frontend_reader:level_one_reader_probe",
        "//src/v/cloud_topics/level_one/maintenance/compaction:compaction_queue",
        "//src/v/cloud_topics/level_one/maintenance/compaction:compaction_source_and_sink",
        "//src/v/cloud_topics/level_one/maintenance/leveling:leveling_queue",
        "//src/v/cloud_topics/level_one/maintenance/leveling:leveling_sink",
        "//src/v/cloud_topics/level_one/maintenance/leveling:leveling_source",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:offset_interval_map",
        "//src/v/cloud_topics/level_one/metastore:replicated_metastore",
        "//src/v/cloud_topics/level_zero/notifier:level_zero_notifier",
        "//src/v/cluster",
        "//src/v/cluster:fwd",
        "//src/v/cluster:members_table",
        "//src/v/cluster:partition_leaders_table",
        "//src/v/cluster:topic_table",
        "//src/v/compaction:key_offset_map",
        "//src/v/compaction:reducer",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/resource_mgmt:cpu_scheduling",
        "//src/v/resource_mgmt:memory_groups",
        "//src/v/ssx:future_util",
        "//src/v/ssx:semaphore",
        "//src/v/ssx:work_queue",
        "//src/v/utils:adjustable_semaphore",
        "//src/v/utils:prefix_logger",
        "@abseil-cpp//absl/hash",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "meta",
    hdrs = [
        "meta.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:leveling_range_builder",
        "//src/v/cloud_topics/level_one/metastore:offset_interval_map",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:intrusive",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "keyed_priority_queue",
    hdrs = [
        "keyed_priority_queue.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
    ],
)

redpanda_cc_library(
    name = "log_collector",
    srcs = [
        "log_collector.cc",
    ],
    hdrs = [
        "log_collector.h",
    ],
    implementation_deps = [
        "//src/v/cluster:notification",
        "//src/v/cluster:topic_configuration",
        "//src/v/cluster:types",
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/cluster:fwd",
        "//src/v/cluster:members_table",
        "//src/v/cluster:partition_leaders_table",
        "//src/v/cluster:partition_properties_stm",
        "//src/v/cluster:state_machine_registry",
        "//src/v/cluster:topic_table",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "log_info_collector",
    srcs = [
        "log_info_collector.cc",
    ],
    hdrs = [
        "log_info_collector.h",
    ],
    implementation_deps = [
        "//src/v/cluster:shard_table",
        "//src/v/cluster:types",
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":logger",
        ":meta",
        "//src/v/cloud_topics/level_one/maintenance/compaction:compaction_queue",
        "//src/v/cloud_topics/level_one/maintenance/leveling:leveling_queue",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cluster",
        "//src/v/cluster:fwd",
        "//src/v/cluster:members_table",
        "//src/v/cluster:partition_leaders_table",
        "//src/v/cluster:partition_properties_stm",
        "//src/v/cluster:state_machine_registry",
        "//src/v/cluster:topic_table",
        "//src/v/compaction:utils",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "scheduling_policies",
    srcs = [
        "scheduling_policies.cc",
    ],
    hdrs = [
        "scheduling_policies.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        ":meta",
        "//src/v/base",
        "//src/v/config",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "scheduler",
    srcs = [
        "scheduler.cc",
    ],
    hdrs = [
        "scheduler.h",
    ],
    implementation_deps = [
        "//src/v/cluster:shard_table",
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":log_collector",
        ":log_info_collector",
        ":logger",
        ":meta",
        ":scheduler_probe",
        ":scheduling_policies",
        ":worker",
        "//src/v/cloud_topics/level_one/common:file_io",
        "//src/v/cloud_topics/level_one/frontend_reader:level_one_reader_probe",
        "//src/v/cloud_topics/level_one/maintenance/compaction:compaction_queue",
        "//src/v/cloud_topics/level_one/maintenance/leveling:leveling_queue",
        "//src/v/cloud_topics/level_one/metastore:replicated_metastore",
        "//src/v/cluster:fwd",
        "//src/v/cluster:members_table",
        "//src/v/cluster:partition_leaders_table",
        "//src/v/cluster:partition_properties_stm",
        "//src/v/cluster:state_machine_registry",
        "//src/v/cluster:topic_table",
        "//src/v/config",
        "//src/v/container:intrusive",
        "//src/v/model",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "scheduler_probe",
    srcs = [
        "scheduler_probe.cc",
    ],
    hdrs = [
        "scheduler_probe.h",
    ],
    deps = [
        "//src/v/config",
        "//src/v/metrics",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "worker_probe",
    srcs = [
        "worker_probe.cc",
    ],
    hdrs = [
        "worker_probe.h",
    ],
    deps = [
        "//src/v/compaction:types",
        "//src/v/config",
        "//src/v/metrics",
        "//src/v/utils:log_hist",
        "@seastar",
    ],
)
