load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "gtest",
    srcs = [
        "gtest_main.cc",
        "gtest_utils.cc",
    ],
    hdrs = [
        "gtest_utils.h",
        "test.h",
    ],
    include_prefix = "test_utils",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_library(
    name = "seastar_boost",
    srcs = [
        "seastar_testing_main.cc",
    ],
    hdrs = [
        "async.h",
        "fixture.h",
        "test_macros.h",
    ],
    include_prefix = "test_utils",
    visibility = ["//visibility:public"],
    deps = [
        "@seastar//:testing",
    ],
)

redpanda_cc_library(
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
