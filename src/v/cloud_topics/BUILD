load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics/batch_cache:__pkg__",
    "//src/v/cloud_topics/core:__pkg__",
    "//src/v/cloud_topics/core/tests:__pkg__",
    "//src/v/cloud_topics/frontend:__pkg__",
    "//src/v/cloud_topics/level_zero:__subpackages__",
    "//src/v/cloud_topics/tests:__pkg__",
    "//src/v/kafka/data:__pkg__",
])

redpanda_cc_library(
    name = "logger",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "test_fixture_cfg",
    hdrs = [
        "test_fixture_cfg.h",
    ],
    visibility = [
        "//src/v/cloud_topics/tests:__pkg__",
        "//src/v/redpanda:__pkg__",
        "//src/v/redpanda/tests:__pkg__",
    ],
)

redpanda_cc_library(
    name = "types",
    srcs = [
        "types.cc",
    ],
    hdrs = [
        "errc.h",
        "types.h",
    ],
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/base",
        "//src/v/random:generators",
        "//src/v/serde",
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "data_plane_api",
    hdrs = [
        "data_plane_api.h",
    ],
    visibility = [
        "//src/v/cloud_topics/frontend:__pkg__",
        "//src/v/cloud_topics/frontend/tests:__pkg__",
        "//src/v/cloud_topics/level_one/frontend_reader:__pkg__",
        "//src/v/cloud_topics/level_one/frontend_reader/tests:__pkg__",
        "//src/v/cloud_topics/level_zero/frontend_reader:__pkg__",
        "//src/v/cloud_topics/reconciler:__pkg__",
        "//src/v/kafka/data:__pkg__",
        "//src/v/redpanda:__pkg__",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/storage",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "app",
    srcs = [
        "app.cc",
    ],
    hdrs = [
        "app.h",
    ],
    implementation_deps = [
        ":cluster_services_interface",
        ":data_plane_impl",
        ":logger",
        ":topic_manifest_upload_manager",
        "//src/v/cloud_topics/housekeeper:manager",
        "//src/v/cloud_topics/level_one/metastore:flush_loop",
        "//src/v/cloud_topics/level_one/metastore:topic_purger",
        "//src/v/cloud_topics/level_zero/gc:level_zero_gc",
        "//src/v/cloud_topics/manager",
        "//src/v/cloud_topics/read_replica:metadata_manager",
        "//src/v/cloud_topics/read_replica:snapshot_manager",
        "//src/v/cluster/utils:partition_change_notifier_impl",
        "//src/v/ssx:future_util",
        "//src/v/utils:directory_walker",
    ],
    visibility = [
        "//src/v/cloud_topics/frontend:__pkg__",
        "//src/v/cloud_topics/frontend/tests:__pkg__",
        "//src/v/cloud_topics/level_one/compaction/tests:__pkg__",
        "//src/v/cloud_topics/level_one/metastore/tests:__pkg__",
        "//src/v/cloud_topics/level_zero/reader:__pkg__",
        "//src/v/cloud_topics/reconciler:__pkg__",
        "//src/v/cloud_topics/tests:__pkg__",
        "//src/v/kafka/data:__pkg__",
        "//src/v/redpanda:__pkg__",
    ],
    deps = [
        ":data_plane_api",
        "//src/v/base",
        "//src/v/cloud_topics:state_accessors",
        "//src/v/cloud_topics/level_one/common:file_io",
        "//src/v/cloud_topics/level_one/compaction:scheduler",
        "//src/v/cloud_topics/level_one/domain:domain_supervisor",
        "//src/v/cloud_topics/level_one/frontend_reader:l1_reader_cache",
        "//src/v/cloud_topics/level_one/frontend_reader:level_one_reader_probe",
        "//src/v/cloud_topics/level_one/metastore:leader_router",
        "//src/v/cloud_topics/level_one/metastore:replicated_metastore",
        "//src/v/cloud_topics/level_zero/cluster_services_impl",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/reconciler",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/resource_mgmt:cpu_scheduling",
        "//src/v/ssx:sharded_service_container",
        "//src/v/storage",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "data_plane_impl",
    srcs = [
        "data_plane_impl.cc",
    ],
    hdrs = [
        "data_plane_impl.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/cloud_io:cache",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:cluster_services_interface",
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics/batch_cache",
        "//src/v/cloud_topics/level_zero/batcher",
        "//src/v/cloud_topics/level_zero/cluster_services_impl",
        "//src/v/cloud_topics/level_zero/pipeline:read_pipeline",
        "//src/v/cloud_topics/level_zero/pipeline:write_pipeline",
        "//src/v/cloud_topics/level_zero/read_fanout",
        "//src/v/cloud_topics/level_zero/read_merge",
        "//src/v/cloud_topics/level_zero/read_request_scheduler",
        "//src/v/cloud_topics/level_zero/reader:fetch_handler",
        "//src/v/cloud_topics/level_zero/write_request_scheduler",
        "//src/v/config",
        "//src/v/model",
        "//src/v/ssx:sharded_service_container",
        "//src/v/storage",
    ],
    visibility = ["//src/v/redpanda:__pkg__"],
    deps = [
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "object_utils",
    srcs = [
        "object_utils.cc",
    ],
    hdrs = [
        "object_utils.h",
    ],
    implementation_deps = [
        "//src/v/base",
    ],
    visibility = [
        ":__subpackages__",
        "//src/v/cloud_topics/reconciler:__pkg__",
    ],
    deps = [
        ":cluster_services_interface",
        ":types",
        "//src/v/base",
        "//src/v/cloud_storage_clients",
        "//src/v/container:chunked_vector",
        "//src/v/ssx:sformat",
        "//src/v/utils:uuid",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "state_accessors",
    hdrs = [
        "state_accessors.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_topics/level_one/frontend_reader:level_one_reader_probe",
    ],
)

redpanda_cc_library(
    name = "topic_id_partition",
    hdrs = [
        "topic_id_partition.h",
    ],
    visibility = [":__subpackages__"],
    deps = [
        ":state_accessors",
        "//src/v/cluster",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "cluster_services_interface",
    hdrs = [
        "cluster_services.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "ephemeral_cluster_services",
    hdrs = [
        "ephemeral_cluster_services.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":cluster_services_interface",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "log_reader_config",
    srcs = [
        "log_reader_config.cc",
    ],
    hdrs = [
        "log_reader_config.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "//src/v/base",
        "//src/v/model",
        "//src/v/utils:to_string",
    ],
)

redpanda_cc_library(
    name = "topic_manifest_uploader",
    srcs = [
        "topic_manifest_uploader.cc",
    ],
    hdrs = [
        "topic_manifest_uploader.h",
    ],
    deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage:topic_manifest_state",
        "//src/v/cloud_storage:topic_path_provider",
        "//src/v/cloud_storage_clients",
        "//src/v/cluster:topic_configuration",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/ssx:future_util",
        "//src/v/utils:detailed_error",
        "//src/v/utils:retry_chain_node",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "topic_manifest_upload_manager",
    srcs = [
        "topic_manifest_upload_manager.cc",
    ],
    hdrs = [
        "topic_manifest_upload_manager.h",
    ],
    implementation_deps = [
        ":topic_manifest_uploader",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage:topic_path_provider",
        "//src/v/cluster",
        "//src/v/utils:retry_chain_node",
    ],
    deps = [
        ":logger",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "//src/v/ssx:actor",
        "//src/v/ssx:work_queue",
        "@seastar",
    ],
)
