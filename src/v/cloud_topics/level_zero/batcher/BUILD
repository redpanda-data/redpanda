load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    ":__subpackages__",
    "//src/v/cloud_topics:__pkg__",
])

redpanda_cc_library(
    name = "aggregator",
    srcs = [
        "aggregator.cc",
    ],
    hdrs = [
        "aggregator.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/pipeline:serializer",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/storage:record_batch_builder",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_topics:cluster_services_interface",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/pipeline:write_request",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "batcher",
    srcs = [
        "batcher.cc",
    ],
    hdrs = [
        "batcher.h",
    ],
    implementation_deps = [
        ":aggregator",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:object_utils",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/pipeline:serializer",
        "//src/v/ssx:sformat",
        "//src/v/utils:human",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics:cluster_services_interface",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/cluster_services_impl",
        "//src/v/cloud_topics/level_zero/pipeline:event_filter",
        "//src/v/cloud_topics/level_zero/pipeline:pipeline_stage",
        "//src/v/cloud_topics/level_zero/pipeline:write_pipeline",
        "//src/v/cloud_topics/level_zero/pipeline:write_request",
        "//src/v/config",
        "//src/v/model",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)
