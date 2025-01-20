load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics:__pkg__",
    "//src/v/cloud_topics/L0_read_path:__pkg__",
    "//src/v/cloud_topics/L0_read_path/tests:__pkg__",
])

redpanda_cc_library(
    name = "placeholder_extent",
    srcs = [
        "placeholder_extent.cc",
    ],
    hdrs = [
        "placeholder_extent.h",
    ],
    include_prefix = "cloud_topics/L0_read_path",
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_io:basic_cache_service_api",
        "//src/v/cloud_io:io_result",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:placeholder",
        "//src/v/cloud_topics:types",
        "//src/v/config",
        "//src/v/model",
        "//src/v/ssx:sformat",
        "//src/v/storage",
        "//src/v/storage:record_batch_utils",
        "//src/v/utils:human",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "placeholder_extent_reader",
    srcs = [
        "placeholder_extent_reader.cc",
    ],
    hdrs = [
        "placeholder_extent_reader.h",
    ],
    include_prefix = "cloud_topics/L0_read_path",
    deps = [
        "//src/v/base",
        "//src/v/cloud_io:basic_cache_service_api",
        "//src/v/cloud_io:io_result",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:placeholder",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/L0_read_path:placeholder_extent",
        "//src/v/config",
        "//src/v/model",
        "//src/v/storage",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "L0_fetch_handler",
    srcs = [
        "L0_fetch_handler.cc",
    ],
    hdrs = [
        "L0_fetch_handler.h",
    ],
    include_prefix = "cloud_topics/L0_read_path",
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_io:basic_cache_service_api",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:placeholder",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/L0_read_path:placeholder_extent_reader",
        "//src/v/cloud_topics/core:event_filter",
        "//src/v/cloud_topics/core:read_pipeline",
        "//src/v/cloud_topics/core:read_request",
        "//src/v/cloud_topics/interfaces:cluster_partition_manager",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/storage",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)
