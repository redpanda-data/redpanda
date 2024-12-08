load("//bazel:test.bzl", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "fixture",
    hdrs = [
        "async.h",
        "fixture.h",
        "logs.h",
        "test_macros.h",
        "tmp_dir.h",
    ],
    include_prefix = "test_utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/model",
        "//src/v/storage",
        "@seastar//:testing",
    ],
)

redpanda_test_cc_library(
    name = "gtest",
    srcs = [
        "gtest_main.cc",
        "gtest_utils.cc",
    ],
    hdrs = [
        "async.h",
        "fixture.h",
        "gtest_utils.h",
        "logs.h",
        "test.h",
        "test_macros.h",
        "tmp_dir.h",
    ],
    include_prefix = "test_utils",
    visibility = ["//visibility:public"],
    deps = [
        ":fixture",
        "//src/v/base",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_test_cc_library(
    name = "seastar_boost",
    srcs = [
        "seastar_testing_main.cc",
    ],
    hdrs = [
        "async.h",
        "fixture.h",
        "logs.h",
        "test_macros.h",
        "tmp_dir.h",
    ],
    include_prefix = "test_utils",
    visibility = ["//visibility:public"],
    deps = [
        ":boost_result_redirect",
        ":fixture",
        "@seastar//:testing",
    ],
)

redpanda_test_cc_library(
    name = "boost_result_redirect",
    srcs = [
        "boost_result_redirect.cc",
    ],
    include_prefix = "test_utils",
    visibility = ["//visibility:public"],
    deps = [
        "@boost//:config",
        "@boost//:core",
        "@boost//:test",
    ],
)

redpanda_test_cc_library(
    name = "random",
    hdrs = [
        "randoms.h",
    ],
    include_prefix = "test_utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:fragmented_vector",
        "//src/v/random:generators",
        "//src/v/utils:tristate",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:node_hash_map",
        "@abseil-cpp//absl/container:node_hash_set",
    ],
)

redpanda_test_cc_library(
    name = "scoped_config",
    hdrs = [
        "scoped_config.h",
    ],
    include_prefix = "test_utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/config",
    ],
)

redpanda_test_cc_library(
    name = "runfiles",
    srcs = [
        "runfiles.cc",
    ],
    hdrs = [
        "runfiles.h",
    ],
    implementation_deps = [
        "@bazel_tools//tools/cpp/runfiles",
        "@fmt",
    ],
    include_prefix = "test_utils",
    # TODO(bazel) when cmake build is removed then this can be removed, and its
    # associated used in runfiles.cc.
    local_defines = [
        "BAZEL_TEST=1",
    ],
    visibility = ["//visibility:public"],
)

redpanda_test_cc_library(
    name = "iostream",
    hdrs = [
        "iostream.h",
    ],
    include_prefix = "test_utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "rpc",
    hdrs = [
        "rpc.h",
    ],
    include_prefix = "test_utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/reflection:adl",
        "//src/v/serde",
    ],
)

redpanda_test_cc_library(
    name = "archival",
    hdrs = [
        "archival.h",
    ],
    include_prefix = "test_utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/model/tests:random",
        "//src/v/storage/tests:disk_log_builder",
        "@boost//:test",
    ],
)

redpanda_test_cc_library(
    name = "tmpbuf_file",
    hdrs = [
        "tmpbuf_file.h",
    ],
    include_prefix = "test_utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
)
