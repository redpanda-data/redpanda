load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics/level_one:__subpackages__",
])

redpanda_cc_library(
    name = "chunk_reassembler",
    srcs = ["chunk_reassembler.cc"],
    hdrs = ["chunk_reassembler.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/container:chunked_vector",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "l1_memory_broker",
    srcs = ["l1_memory_broker.cc"],
    hdrs = ["l1_memory_broker.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/config",
    ],
)

redpanda_cc_library(
    name = "prefetch_pacer",
    srcs = ["prefetch_pacer.cc"],
    hdrs = ["prefetch_pacer.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "chunk_downloader",
    srcs = ["chunk_downloader.cc"],
    hdrs = ["chunk_downloader.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":chunk_reassembler",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_io:scheduler_types",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "fetch_stream",
    srcs = ["fetch_stream.cc"],
    hdrs = ["fetch_stream.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":chunk_downloader",
        ":prefetch_pacer",
        "//src/v/base",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/batch_cache",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/frontend_reader:level_one_reader_probe",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/cloud_topics/level_one/metastore:retry",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/model:batch_utils",
        "//src/v/storage:batch_cache",
        "//src/v/utils:prefix_logger",
        "//src/v/utils:retry_chain_node",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "memory_first_reader",
    srcs = ["memory_first_reader.cc"],
    hdrs = ["memory_first_reader.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":fetch_stream",
        "//src/v/base",
        "//src/v/cloud_topics:log_reader_config",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/batch_cache",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "prefetch_probe",
    srcs = ["prefetch_probe.cc"],
    hdrs = ["prefetch_probe.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/config",
        "//src/v/metrics",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "l1_fetch_service",
    srcs = ["l1_fetch_service.cc"],
    hdrs = ["l1_fetch_service.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":fetch_stream",
        ":l1_memory_broker",
        ":memory_first_reader",
        ":prefetch_pacer",
        ":prefetch_probe",
        "//src/v/base",
        "//src/v/cloud_topics:log_reader_config",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/batch_cache",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "@seastar",
    ],
)
