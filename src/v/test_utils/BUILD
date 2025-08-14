load("//bazel:test.bzl", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "fixture",
    hdrs = [
        "async.h",
        "fixture.h",
        "test_macros.h",
        "tmp_dir.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/model:timeout_clock",
        "//src/v/ssx:sformat",
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
        "gtest_exception.h",
        "gtest_utils.h",
        "test.h",
        "test_macros.h",
        "tmp_dir.h",
    ],
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
        "test_macros.h",
        "tmp_dir.h",
    ],
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
    implementation_deps = [
        "//src/v/thirdparty/libxml2",
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/strings:str_format",
        "@abseil-cpp//absl/time",
        "@boost//:config",
        "@boost//:core",
        "@boost//:test",
    ],
    visibility = ["//visibility:public"],
)

redpanda_test_cc_library(
    name = "random",
    hdrs = [
        "randoms.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/random:generators",
        "//src/v/utils:tristate",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:node_hash_map",
        "@abseil-cpp//absl/container:node_hash_set",
    ],
)

redpanda_test_cc_library(
    name = "random_bytes",
    srcs = ["random_bytes.cc"],
    hdrs = [
        "random_bytes.h",
    ],
    implementation_deps = [
        "//src/v/random:generators",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
    ],
)

redpanda_test_cc_library(
    name = "scoped_config",
    hdrs = [
        "scoped_config.h",
    ],
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
    visibility = ["//visibility:public"],
)

redpanda_test_cc_library(
    name = "iostream",
    hdrs = [
        "iostream.h",
    ],
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
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/reflection:adl",
        "//src/v/serde",
    ],
)

redpanda_test_cc_library(
    name = "logs",
    hdrs = [
        "logs.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model",
        "//src/v/storage",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "archival",
    hdrs = [
        "archival.h",
    ],
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
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "tee_log",
    hdrs = [
        "tee_log.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@boost//:iostreams",
        "@seastar",
    ],
)
