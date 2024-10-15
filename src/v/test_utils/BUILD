load("//bazel:test.bzl", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "gtest",
    srcs = [
        "gtest_main.cc",
        "gtest_utils.cc",
    ],
    hdrs = [
        "async.h",
        "gtest_utils.h",
        "test.h",
        "test_macros.h",
    ],
    include_prefix = "test_utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/model",
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
        "test_macros.h",
        "tmp_dir.h",
    ],
    include_prefix = "test_utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/model",
        "@seastar//:testing",
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
