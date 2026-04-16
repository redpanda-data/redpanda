load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//visibility:public"])

redpanda_cc_library(
    name = "persistence",
    hdrs = ["persistence.h"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:ioarray",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core/internal:files",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "memory_persistence",
    srcs = ["memory_persistence.cc"],
    hdrs = ["memory_persistence.h"],
    deps = [
        ":persistence",
        "//src/v/base",
        "//src/v/bytes:ioarray",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core:exceptions",
        "//src/v/lsm/core/internal:files",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "disk_persistence",
    srcs = ["disk_persistence.cc"],
    hdrs = ["disk_persistence.h"],
    deps = [
        ":file_io",
        ":persistence",
        "//src/v/base",
        "//src/v/bytes:ioarray",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core:exceptions",
        "//src/v/lsm/core/internal:files",
        "//src/v/utils:file_io",
        "//src/v/utils:uuid",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "cloud_persistence",
    srcs = ["cloud_persistence.cc"],
    hdrs = ["cloud_persistence.h"],
    implementation_deps = [
        ":file_io",
        "//src/v/base",
        "//src/v/bytes:ioarray",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_io:io_result",
        "//src/v/config",
        "//src/v/lsm/core:exceptions",
        "//src/v/lsm/core/internal:files",
        "//src/v/ssx:future_util",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:uuid",
    ],
    deps = [
        ":persistence",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "readahead_file_reader",
    srcs = ["readahead_file_reader.cc"],
    hdrs = ["readahead_file_reader.h"],
    deps = [
        ":persistence",
        "//src/v/bytes:ioarray",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "file_io",
    srcs = ["file_io.cc"],
    hdrs = ["file_io.h"],
    visibility = ["//visibility:private"],
    deps = [
        ":persistence",
        "//src/v/base",
        "//src/v/bytes:ioarray",
        "//src/v/bytes:iobuf",
        "//src/v/lsm/core:exceptions",
        "//src/v/lsm/core/internal:files",
        "@fmt",
        "@seastar",
    ],
)
