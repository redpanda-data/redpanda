load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/reader/tests:__pkg__"])

redpanda_cc_library(
    name = "placeholder_extent",
    srcs = [
        "placeholder_extent.cc",
    ],
    hdrs = [
        "placeholder_extent.h",
    ],
    include_prefix = "cloud_topics/reader",
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_io:basic_cache_service_api",
        "//src/v/cloud_io:io_result",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:placeholder",
        "//src/v/cloud_topics:types",
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
    name = "placeholder_extent_reader",
    srcs = [
        "placeholder_extent_reader.cc",
    ],
    hdrs = [
        "placeholder_extent_reader.h",
    ],
    include_prefix = "cloud_topics/reader",
    deps = [
        "//src/v/base",
        "//src/v/cloud_io:basic_cache_service_api",
        "//src/v/cloud_io:io_result",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:placeholder",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/reader:placeholder_extent",
        "//src/v/config",
        "//src/v/model",
        "//src/v/storage",
        "//src/v/utils:retry_chain_node",
        "@seastar",
    ],
)
