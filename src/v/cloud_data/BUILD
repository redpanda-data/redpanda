load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_cc_gtest")
load("//bazel:test.bzl", "redpanda_test_cc_library")

redpanda_cc_library(
    name = "types",
    hdrs = [
        "dl_placeholder.h",
        "dl_stm_cmd.h",
        "errc.h",
        "logger.h",
        "types.h",
    ],
    include_prefix = "cloud_data",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:uuid",
    ],
)

redpanda_cc_library(
    name = "dl_stm",
    srcs = [
        "dl_stm.cc",
    ],
    hdrs = [
        "dl_stm.h",
    ],
    include_prefix = "cloud_data",
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/serde",
        "//src/v/serde:enum",
        "//src/v/serde:named_type",
        "//src/v/serde:uuid",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/utils:uuid",
        "//src/v/ssx:semaphore",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "aggregated_uploader",
    srcs = [
        "aggregated_uploader.cc",
    ],
    hdrs = [
        "aggregated_uploader.h",
    ],
    include_prefix = "cloud_data",
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/config",
        "//src/v/container:fragmented_vector",
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
        "//src/v/cloud_io:remote",
        "//src/v/storage:record_batch_utils",
        "@fmt",
        "@seastar",
        "@abseil-cpp//absl/container:btree",
    ],
)

redpanda_cc_library(
    name = "aggregated_log_reader",
    srcs = [
        "aggregated_log_reader.cc",
    ],
    hdrs = [
        "aggregated_log_reader.h",
    ],
    include_prefix = "cloud_data",
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "//src/v/storage",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/config",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_storage",
        "//src/v/cloud_io:remote",
        "//src/v/container:fragmented_vector",
        "//src/v/model",
        "//src/v/reflection:arity",
        "//src/v/reflection:to_tuple",
        "//src/v/serde",
        "//src/v/serde:uuid",
        "//src/v/serde:named_type",
        "//src/v/serde:iobuf",
        "//src/v/serde:enum",
        "//src/v/storage:record_batch_builder",
        "//src/v/storage:record_batch_utils",
        "//src/v/ssx:sformat",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:uuid",
        # TODO: remove this dependency when cloud_io is merged
        "@fmt",
        "@seastar",
        "@abseil-cpp//absl/container:btree",
    ],
)
