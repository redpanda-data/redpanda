load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics:__pkg__",
    "//src/v/cloud_topics/level_zero/reader:__pkg__",
    "//src/v/cloud_topics/level_zero/reader/tests:__pkg__",
])

redpanda_cc_library(
    name = "materialized_extent",
    srcs = [
        "materialized_extent.cc",
    ],
    hdrs = [
        "materialized_extent.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_io:basic_cache_service_api",
        "//src/v/cloud_io:io_result",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:object_utils",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/common:micro_probe",
        "//src/v/cloud_topics/level_zero/stm:placeholder",
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
    name = "materialized_extent_reader",
    srcs = [
        "materialized_extent_reader.cc",
    ],
    hdrs = [
        "materialized_extent_reader.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_io:basic_cache_service_api",
        "//src/v/cloud_io:io_result",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/common:micro_probe",
        "//src/v/cloud_topics/level_zero/reader:materialized_extent",
        "//src/v/cloud_topics/level_zero/stm:placeholder",
        "//src/v/config",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model",
        "//src/v/storage",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "fetch_handler",
    srcs = [
        "fetch_request_handler.cc",
    ],
    hdrs = [
        "fetch_request_handler.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_io:basic_cache_service_api",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/pipeline:event_filter",
        "//src/v/cloud_topics/level_zero/pipeline:read_pipeline",
        "//src/v/cloud_topics/level_zero/pipeline:read_request",
        "//src/v/cloud_topics/level_zero/reader:materialized_extent_reader",
        "//src/v/cloud_topics/level_zero/stm:placeholder",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/storage",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)
