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
    name = "filter",
    srcs = [
        "filter.cc",
    ],
    hdrs = [
        "filter.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/metastore:offset_interval_set",
        "//src/v/compaction:filter",
        "//src/v/compaction:key_offset_map",
        "//src/v/compaction:utils",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "source_and_sink",
    srcs = [
        "sink.cc",
        "source.cc",
    ],
    hdrs = [
        "sink.h",
        "source.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        ":committer",
        ":filter",
        ":logger",
        ":meta",
        ":worker_probe",
        "//src/v/bytes",
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics:log_reader_config",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/frontend_reader:reader",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:extent_metadata_reader",
        "//src/v/cloud_topics/level_one/metastore:offset_interval_set",
        "//src/v/compaction:key",
        "//src/v/compaction:key_offset_map",
        "//src/v/compaction:reducer",
        "//src/v/compaction:utils",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/model:batch_compression",
        "//src/v/ssx:future_util",
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
        ":committer",
        ":logger",
        ":meta",
        ":scheduler_probe",
        ":source_and_sink",
        ":worker_probe",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:file_io",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:replicated_metastore",
        "//src/v/cluster",
        "//src/v/compaction:key_offset_map",
        "//src/v/compaction:reducer",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/resource_mgmt:memory_groups",
        "//src/v/ssx:future_util",
        "//src/v/ssx:work_queue",
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
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:intrusive",
        "//src/v/model",
        "@seastar",
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
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cluster",
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
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":logger",
        ":meta",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cluster",
        "//src/v/compaction:utils",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "committing_policy",
    srcs = [
        "committing_policy.cc",
    ],
    hdrs = [
        "committing_policy.h",
    ],
    implementation_deps = [
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":meta",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "committer",
    srcs = [
        "committer.cc",
    ],
    hdrs = [
        "committer.h",
    ],
    implementation_deps = [
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":committing_policy",
        ":logger",
        ":meta",
        "//src/v/base",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:retry",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/ssx:semaphore",
        "//src/v/ssx:when_all",
        "//src/v/utils:uuid",
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
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":committer",
        ":committing_policy",
        ":log_collector",
        ":log_info_collector",
        ":logger",
        ":meta",
        ":scheduler_probe",
        ":scheduling_policies",
        ":worker",
        "//src/v/cloud_topics/level_one/common:file_io",
        "//src/v/cloud_topics/level_one/metastore:replicated_metastore",
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
