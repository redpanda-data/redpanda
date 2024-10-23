load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics/batcher:__pkg__",
    "//src/v/cloud_topics/batcher/tests:__pkg__",
    "//src/v/cloud_topics/core/tests:__pkg__",
    "//src/v/cloud_topics/throttler:__pkg__",
    "//src/v/cloud_topics/throttler/tests:__pkg__",
])

redpanda_cc_library(
    name = "serializer",
    srcs = [
        "serializer.cc",
    ],
    hdrs = [
        "serializer.h",
    ],
    implementation_deps = [
        "//src/v/storage:record_batch_utils",
    ],
    include_prefix = "cloud_topics/core",
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "write_request",
    srcs = [
        "write_request.cc",
    ],
    hdrs = [
        "write_request.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
    ],
    include_prefix = "cloud_topics/core",
    deps = [
        ":pipeline_stage",
        ":serializer",
        "//src/v/base",
        "//src/v/cloud_topics:types",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "event_filter",
    hdrs = [
        "event_filter.h",
    ],
    include_prefix = "cloud_topics/core",
    deps = [
        ":pipeline_stage",
        "//src/v/base",
        "//src/v/container:intrusive",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "pipeline_stage",
    srcs = [
        "pipeline_stage.cc",
    ],
    hdrs = [
        "pipeline_stage.h",
    ],
    include_prefix = "cloud_topics/core",
    deps = [
        "//src/v/base",
        "//src/v/utils:named_type",
    ],
)

redpanda_cc_library(
    name = "write_pipeline",
    srcs = [
        "write_pipeline.cc",
    ],
    hdrs = [
        "write_pipeline.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/core:serializer",
        "//src/v/ssx:sformat",
        "//src/v/utils:human",
    ],
    include_prefix = "cloud_topics/core",
    deps = [
        ":pipeline_stage",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/core:event_filter",
        "//src/v/cloud_topics/core:write_request",
        "//src/v/config",
        "//src/v/model",
        "//src/v/ssx:semaphore",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)
