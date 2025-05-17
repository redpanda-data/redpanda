load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics:__pkg__",
    "//src/v/cloud_topics/batcher:__pkg__",
    "//src/v/cloud_topics/batcher/tests:__pkg__",
    "//src/v/cloud_topics/core/tests:__pkg__",
    "//src/v/cloud_topics/read_path:__pkg__",
    "//src/v/cloud_topics/read_path/tests:__pkg__",
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
        "//src/v/container:chunked_circular_buffer",
        "//src/v/container:intrusive",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "read_request",
    srcs = [
        "read_request.cc",
    ],
    hdrs = [
        "read_request.h",
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
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/storage",
        "//src/v/utils:retry_chain_node",
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
    name = "circuit_breaker",
    hdrs = [
        "circuit_breaker.h",
    ],
    include_prefix = "cloud_topics/core",
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "base_pipeline",
    hdrs = [
        "base_pipeline.h",
    ],
    include_prefix = "cloud_topics/core",
    deps = [
        ":pipeline_stage",
        "//src/v/base",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/core:event_filter",
        "//src/v/container:fragmented_vector",
        "//src/v/ssx:future_util",
        "//src/v/utils:retry_chain_node",
        "@seastar",
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
        ":base_pipeline",
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

redpanda_cc_library(
    name = "read_pipeline",
    srcs = [
        "read_pipeline.cc",
    ],
    hdrs = [
        "read_pipeline.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/core:serializer",
        "//src/v/ssx:sformat",
        "//src/v/utils:human",
    ],
    include_prefix = "cloud_topics/core",
    deps = [
        ":base_pipeline",
        ":pipeline_stage",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/core:circuit_breaker",
        "//src/v/cloud_topics/core:event_filter",
        "//src/v/cloud_topics/core:read_request",
        "//src/v/config",
        "//src/v/model",
        "//src/v/ssx:semaphore",
        "//src/v/storage",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)
