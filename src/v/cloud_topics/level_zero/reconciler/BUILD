load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [":__subpackages__"])

redpanda_cc_library(
    name = "range_batch_consumer",
    srcs = [
        "range_batch_consumer.cc",
    ],
    hdrs = [
        "range_batch_consumer.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/model",
        "@abseil-cpp//absl/container:btree",
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
    implementation_deps = [
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics/frontend",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":range_batch_consumer",
        "//src/v/base",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics:object_utils",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_one/common:object_utils",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm_api",
        "//src/v/cluster",
        "//src/v/cluster:notification",
        "//src/v/container:chunked_vector",
        "//src/v/kafka/utils:txn_reader",
        "//src/v/model",
        "//src/v/random:generators",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:node_hash_map",
        "@seastar",
    ],
)
