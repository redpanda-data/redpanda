load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/cloud_topics/level_one:__subpackages__"])

redpanda_cc_library(
    name = "logger",
    hdrs = [
        "logger.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "source",
    srcs = [
        "source.cc",
    ],
    hdrs = [
        "source.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        "//src/v/compaction:reducer",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "sink",
    srcs = [
        "sink.cc",
    ],
    hdrs = [
        "sink.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/compaction:reducer",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/model:batch_compression",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "filter",
    srcs = [
        "filter.cc",
    ],
    hdrs = [
        "filter.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        "//src/v/compaction:filter",
        "//src/v/compaction:key_offset_map",
        "//src/v/compaction:utils",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "meta",
    hdrs = [
        "meta.h",
    ],
    visibility = [
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:object",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:intrusive",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "committing_policy",
    srcs = [
        "committing_policy.cc",
    ],
    hdrs = [
        "committing_policy.h",
    ],
    implementation_deps = [
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":meta",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "committer",
    srcs = [
        "committer.cc",
    ],
    hdrs = [
        "committer.h",
    ],
    implementation_deps = [
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":committing_policy",
        ":logger",
        ":meta",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)
