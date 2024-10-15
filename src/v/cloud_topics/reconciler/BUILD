load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/reconciler/tests:__pkg__"])

redpanda_cc_library(
    name = "range_batch_consumer",
    srcs = [
        "range_batch_consumer.cc",
    ],
    hdrs = [
        "range_batch_consumer.h",
    ],
    include_prefix = "cloud_topics/reconciler",
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "reconciler",
    srcs = [
        "reconciler.cc",
    ],
    hdrs = [
        "reconciler.h",
    ],
    include_prefix = "cloud_topics/reconciler",
    visibility = ["//visibility:public"],
    deps = [
        ":range_batch_consumer",
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/cluster:notification",
        "//src/v/container:fragmented_vector",
        "//src/v/kafka/server",
        "//src/v/kafka/utils:txn_reader",
        "//src/v/model",
        "//src/v/random:generators",
        "@abseil-cpp//absl/container:node_hash_map",
        "@seastar",
    ],
)
