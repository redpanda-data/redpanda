load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "scoped_remote",
    srcs = [
        "scoped_remote.cc",
    ],
    hdrs = [
        "scoped_remote.h",
    ],
    include_prefix = "cloud_io/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
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
        "@boost//:algorithm",
        "@boost//:core",
        "@boost//:property_tree",
    ],
    include_prefix = "cloud_io/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/http/tests:utils",
        "//src/v/model/tests:random",
        "//src/v/test_utils:fixture",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_gtest(
    name = "io_resources_test",
    timeout = "short",
    srcs = [
        "io_resources_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_io:io_resources",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
