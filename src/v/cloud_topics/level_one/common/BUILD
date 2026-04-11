load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/cloud_topics/level_one:__subpackages__",
    "//src/v/cloud_topics/reconciler:__subpackages__",
])

redpanda_cc_library(
    name = "object_id",
    srcs = [
        "object_id.cc",
    ],
    hdrs = [
        "object_id.h",
    ],
    visibility = [
        "//src/v/cloud_topics/housekeeper/tests:__subpackages__",
        "//src/v/cloud_topics/level_one:__subpackages__",
        "//src/v/cloud_topics/reconciler:__subpackages__",
    ],
    deps = [
        "//src/v/base",
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
        "@fmt",
    ],
)

redpanda_cc_library(
    name = "object_utils",
    srcs = [
        "object_utils.cc",
    ],
    hdrs = [
        "object_utils.h",
    ],
    deps = [
        ":object_id",
        "//src/v/cloud_storage_clients",
        "//src/v/ssx:sformat",
    ],
)

redpanda_cc_library(
    name = "object",
    srcs = [
        "object.cc",
    ],
    hdrs = [
        "object.h",
    ],
    implementation_deps = [
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/reflection:type_traits",
        "//src/v/serde:vector",
        "//src/v/storage:record_batch_builder",
    ],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/serde",
        "@abseil-cpp//absl/container:btree",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "abstract_io",
    srcs = ["abstract_io.cc"],
    hdrs = ["abstract_io.h"],
    implementation_deps = [
        "//src/v/bytes:iostream",
    ],
    deps = [
        ":object_id",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_storage_clients",
        "//src/v/container:chunked_vector",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "file_io",
    srcs = ["file_io.cc"],
    hdrs = ["file_io.h"],
    visibility = [
        "//src/v/cloud_topics:__pkg__",
        "//src/v/cloud_topics/level_one:__subpackages__",
        "//src/v/cloud_topics/read_replica:__pkg__",
    ],
    deps = [
        ":abstract_io",
        ":object_id",
        ":object_utils",
        "//src/v/cloud_io:cache",
        "//src/v/cloud_io:io_result",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics:logger",
        "//src/v/config",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "fake_io",
    srcs = ["fake_io.cc"],
    hdrs = ["fake_io.h"],
    deps = [
        ":abstract_io",
        ":object_id",
        ":object_utils",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage_clients",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "file_arena",
    srcs = [
        "file_arena.cc",
        "file_arena_probe.cc",
    ],
    hdrs = [
        "file_arena.h",
        "file_arena_probe.h",
    ],
    implementation_deps = [
        "//src/v/ssx:future_util",
        "//src/v/utils:human",
    ],
    visibility = [
        "//src/v/cloud_topics:__pkg__",
        "//src/v/cloud_topics/level_one:__subpackages__",
    ],
    deps = [
        ":abstract_io",
        "//src/v/config",
        "//src/v/metrics",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)
