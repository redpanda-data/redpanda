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
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:enum",
        "//src/v/utils:named_type",
        "//src/v/utils:uuid",
        "@fmt",
        "@seastar",
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
        "//src/v/cloud_io:admission_control_types",
        "//src/v/cloud_storage_clients",
        "//src/v/container:chunked_vector",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "single_flight",
    srcs = ["single_flight.cc"],
    hdrs = ["single_flight.h"],
    deps = [
        ":abstract_io",
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "object_handle",
    srcs = ["object_handle.cc"],
    hdrs = ["object_handle.h"],
    deps = [
        ":abstract_io",
        ":object",
        ":object_id",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "open_object",
    srcs = ["open_object.cc"],
    hdrs = ["open_object.h"],
    implementation_deps = [
        ":chunk_data_source",
        ":object",
        "//src/v/cloud_topics:logger",
        "//src/v/config",
    ],
    deps = [
        ":abstract_io",
        ":object_handle",
        ":object_id",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "file_io",
    srcs = [
        "file_io.cc",
        "file_io_probe.cc",
    ],
    hdrs = [
        "file_io.h",
        "file_io_probe.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/bytes:iostream",
    ],
    visibility = [
        "//src/v/cloud_topics:__pkg__",
        "//src/v/cloud_topics/level_one:__subpackages__",
        "//src/v/cloud_topics/read_replica:__pkg__",
    ],
    deps = [
        ":abstract_io",
        ":object_id",
        ":object_utils",
        ":single_flight",
        "//src/v/cloud_io:cache",
        "//src/v/cloud_io:io_result",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics:logger",
        "//src/v/config",
        "//src/v/metrics",
        "//src/v/model",
        "//src/v/utils:retry_chain_node",
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
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage_clients",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "chunk_data_source",
    hdrs = ["chunk_data_source.h"],
    deps = [
        "//src/v/base",
        "@fmt",
        "@seastar",
    ],
)
