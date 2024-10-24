load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/batcher/tests:__pkg__"])

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
    include_prefix = "cloud_topics/batcher",
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
    include_prefix = "cloud_topics/batcher",
    deps = [
        ":serializer",
        "//src/v/base",
        "//src/v/cloud_topics:types",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "aggregator",
    srcs = [
        "aggregator.cc",
    ],
    hdrs = [
        "aggregator.h",
    ],
    implementation_deps = [
        ":serializer",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:placeholder",
        "//src/v/storage:record_batch_builder",
    ],
    include_prefix = "cloud_topics/batcher",
    deps = [
        ":write_request",
        "//src/v/base",
        "//src/v/cloud_topics:types",
        "//src/v/container:fragmented_vector",
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
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/batcher:aggregator",
        "//src/v/cloud_topics/batcher:serializer",
        "//src/v/ssx:sformat",
        "//src/v/utils:human",
    ],
    include_prefix = "cloud_topics/batcher",
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/batcher:write_request",
        "//src/v/config",
        "//src/v/model",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)
