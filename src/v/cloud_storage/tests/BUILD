load(
    "//bazel:test.bzl",
    "redpanda_cc_btest",
    "redpanda_cc_gtest",
    "redpanda_test_cc_library",
)

redpanda_test_cc_library(
    name = "s3_imposter_gtest",
    srcs = [
        "s3_imposter.cc",
    ],
    hdrs = [
        "s3_imposter.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/test_utils:gtest",
    ],
    include_prefix = "cloud_storage/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/http/tests:utils",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "s3_imposter",
    srcs = [
        "s3_imposter.cc",
    ],
    hdrs = [
        "s3_imposter.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/test_utils:seastar_boost",
    ],
    include_prefix = "cloud_storage/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/http/tests:utils",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "common",
    hdrs = [
        "cloud_storage_fixture.h",
        "common_def.h",
        "util.h",
    ],
    include_prefix = "cloud_storage/tests",
    deps = [
        ":s3_imposter",
        "//src/v/bytes:iobuf",
        "//src/v/cloud_storage",
        "//src/v/model/tests:random",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:seastar_boost",
        "@boost//:numeric_conversion",
    ],
)

redpanda_cc_btest(
    name = "segment_meta_cstore_test",
    timeout = "short",
    srcs = [
        "segment_meta_cstore_test.cc",
    ],
    deps = [
        ":common",
        "//src/v/base",
        "//src/v/cloud_storage",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/utils:delta_for",
        "//src/v/utils:human",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "segment_chunk_test",
    timeout = "short",
    srcs = [
        "segment_chunk_test.cc",
    ],
    deps = [
        "//src/v/cloud_storage",
        "//src/v/test_utils:seastar_boost",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "partition_manifest_test",
    timeout = "short",
    srcs = [
        "partition_manifest_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:tracking_allocator",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "directory_walker_test",
    timeout = "short",
    srcs = [
        "directory_walker_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "remote_segment_index_test",
    timeout = "short",
    srcs = [
        "remote_segment_index_test.cc",
    ],
    deps = [
        ":common",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/model",
        "//src/v/random:generators",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "recovery_request_test",
    timeout = "short",
    srcs = [
        "recovery_request_test.cc",
    ],
    deps = [
        "//src/v/cloud_storage",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "materialized_manifest_cache_test",
    timeout = "short",
    srcs = [
        "materialized_manifest_cache_test.cc",
    ],
    deps = [
        ":common",
        ":s3_imposter",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:retry_chain_node",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "remote_path_provider_test",
    timeout = "short",
    srcs = [
        "remote_path_provider_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_storage",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "//src/v/utils:uuid",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "topic_mount_handler_test",
    timeout = "short",
    srcs = [
        "topic_mount_handler_test.cc",
    ],
    cpu = 1,
    deps = [
        ":s3_imposter_gtest",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage_clients",
        "//src/v/cluster:topic_configuration",
        "//src/v/config",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "//src/v/utils:retry_chain_node",
        "//src/v/utils:uuid",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_btest(
    name = "cache_test",
    timeout = "short",
    srcs = [
        "cache_test.cc",
        "cache_test_fixture.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/config",
        "//src/v/random:generators",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:file_io",
        "//src/v/utils:human",
        "@boost//:filesystem",
        "@boost//:test",
        "@seastar",
    ],
)
