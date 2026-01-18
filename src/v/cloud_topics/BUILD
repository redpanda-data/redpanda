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
        "//src/v/random:generators",
        "//src/v/serde",
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
        "@fmt",
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
        "//src/v/cloud_topics/housekeeper:manager",
        "//src/v/cloud_topics/level_one/metastore:topic_purger",
        "//src/v/cloud_topics/level_zero/gc:level_zero_gc",
        "//src/v/cloud_topics/manager",
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
        "//src/v/cloud_topics/level_one/metastore:leader_router",
        "//src/v/cloud_topics/level_one/metastore:replicated_metastore",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/reconciler",
        "//src/v/cluster",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/model",
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
        "//src/v/cloud_topics/level_zero/read_debounce",
        "//src/v/cloud_topics/level_zero/read_fanout",
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
        "//src/v/base",
        "//src/v/model",
        "//src/v/utils:to_string",
    ],
)
