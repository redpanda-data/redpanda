load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    ":__subpackages__",
    "//src/v/cloud_topics:__pkg__",
    "//src/v/cloud_topics/level_zero/batcher:__pkg__",
    "//src/v/cloud_topics/level_zero/batcher/tests:__pkg__",
    "//src/v/cloud_topics/level_zero/read_fanout:__pkg__",
    "//src/v/cloud_topics/level_zero/read_fanout/tests:__pkg__",
    "//src/v/cloud_topics/level_zero/read_merge:__pkg__",
    "//src/v/cloud_topics/level_zero/read_merge/tests:__pkg__",
    "//src/v/cloud_topics/level_zero/read_request_scheduler:__pkg__",
    "//src/v/cloud_topics/level_zero/read_request_scheduler/tests:__pkg__",
    "//src/v/cloud_topics/level_zero/reader:__pkg__",
    "//src/v/cloud_topics/level_zero/reader/tests:__pkg__",
    "//src/v/cloud_topics/level_zero/tests:__pkg__",
    "//src/v/cloud_topics/level_zero/write_request_scheduler:__pkg__",
    "//src/v/cloud_topics/level_zero/write_request_scheduler/tests:__pkg__",
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
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "circuit_breaker",
    hdrs = [
        "circuit_breaker.h",
    ],
    deps = [
        "//src/v/base",
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
    deps = [
        "//src/v/base",
        "//src/v/utils:named_type",
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
    deps = [
        ":pipeline_stage",
        ":serializer",
        "//src/v/base",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/container:chunked_vector",
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
    deps = [
        ":pipeline_stage",
        ":serializer",
        "//src/v/base",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/common:micro_probe",
        "//src/v/container:chunked_vector",
        "//src/v/container:intrusive",
        "//src/v/model",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "event_filter",
    hdrs = [
        "event_filter.h",
    ],
    deps = [
        ":pipeline_stage",
        "//src/v/base",
        "//src/v/container:intrusive",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "base_pipeline",
    hdrs = [
        "base_pipeline.h",
    ],
    deps = [
        ":event_filter",
        ":pipeline_stage",
        "//src/v/base",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/container:chunked_vector",
        "//src/v/ssx:future_util",
        "//src/v/utils:retry_chain_node",
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
        ":serializer",
        "//src/v/cloud_topics:logger",
        "//src/v/resource_mgmt:memory_groups",
        "//src/v/ssx:abort_source",
        "//src/v/ssx:sformat",
        "//src/v/utils:human",
    ],
    deps = [
        ":base_pipeline",
        ":circuit_breaker",
        ":event_filter",
        ":pipeline_stage",
        ":read_request",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/common:level_zero_probe",
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

redpanda_cc_library(
    name = "write_pipeline",
    srcs = [
        "write_pipeline.cc",
    ],
    hdrs = [
        "write_pipeline.h",
    ],
    implementation_deps = [
        ":serializer",
        "//src/v/cloud_topics:logger",
        "//src/v/resource_mgmt:memory_groups",
        "//src/v/ssx:sformat",
        "//src/v/utils:human",
    ],
    deps = [
        ":base_pipeline",
        ":event_filter",
        ":pipeline_stage",
        ":write_request",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/common:level_zero_probe",
        "//src/v/config",
        "//src/v/model",
        "//src/v/ssx:semaphore",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)
