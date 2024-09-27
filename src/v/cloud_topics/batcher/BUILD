load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/batcher/tests:__pkg__"])

redpanda_cc_library(
    name="serializer",
    srcs = [
        "serializer.cc",
    ],
    hdrs = [
        "serializer.h",
    ],
    include_prefix="cloud_topics/batcher",
    deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/model",
        "//src/v/storage:record_batch_utils",
    ],
)

redpanda_cc_library(
    name="write_request",
    srcs = [
        "write_request.cc",
    ],
    hdrs = [
        "write_request.h",
    ],
    include_prefix="cloud_topics/batcher",
    deps = [
        ":serializer",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics:logger",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name="aggregator",
    srcs = [
        "aggregator.cc",
    ],
    hdrs = [
        "aggregator.h",
    ],
    include_prefix="cloud_topics/batcher",
    deps = [
        ":write_request",
        ":serializer",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:placeholder",
        "//src/v/storage:record_batch_builder",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iostream",
        "//src/v/model",
        "@seastar",
        "@abseil-cpp//absl/container:btree",
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
    include_prefix = "cloud_topics/batcher",
    deps = [
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics:placeholder",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/batcher:write_request",
        "//src/v/cloud_topics/batcher:serializer",
        "//src/v/cloud_topics/batcher:aggregator",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/config",
        "//src/v/container:fragmented_vector",
        "//src/v/container:intrusive",
        "//src/v/model",
        "//src/v/reflection:arity",
        "//src/v/reflection:to_tuple",
        "//src/v/serde",
        "//src/v/serde:uuid",
        "//src/v/serde:named_type",
        "//src/v/serde:iobuf",
        "//src/v/serde:enum",
        "//src/v/storage:record_batch_builder",
        "//src/v/ssx:sformat",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:uuid",
        "//src/v/utils:human",
        "//src/v/cloud_io:remote",
        "//src/v/storage:record_batch_utils",
        "@fmt",
        "@seastar",
        "@abseil-cpp//absl/container:btree",
    ],
)