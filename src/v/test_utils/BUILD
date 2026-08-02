load("//bazel:test.bzl", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "gtest",
    srcs = [
        "gtest_main.cc",
        "gtest_utils.cc",
    ],
    hdrs = [
        "async.h",
        "gtest_exception.h",
        "gtest_utils.h",
        "test.h",
        "test_macros.h",
        "tmp_dir.h",
    ],
    implementation_deps = [
        ":global_test_hooks",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/model:timeout_clock",
        "//src/v/ssx:sformat",
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
        "boost_fixture.h",
        "test_macros.h",
        "tmp_dir.h",
    ],
    local_defines = ["IS_BTEST"],
    visibility = ["//visibility:public"],
    deps = [
        ":boost_result_redirect",
        "//src/v/base",
        "//src/v/model:timeout_clock",
        "//src/v/ssx:sformat",
        "@seastar//:testing",
    ],
)

redpanda_test_cc_library(
    name = "boost_result_redirect",
    srcs = [
        "boost_result_redirect.cc",
    ],
    implementation_deps = [
        "@abseil-cpp//absl/strings",
        "@abseil-cpp//absl/strings:str_format",
        "@abseil-cpp//absl/time",
        "@boost//:config",
        "@boost//:core",
        "@boost//:test",
        "@libxml2",
    ],
    visibility = ["//visibility:public"],
)

redpanda_test_cc_library(
    name = "test_macros",
    hdrs = [
        "test_macros.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
    ],
)

redpanda_test_cc_library(
    name = "tmp_dir",
    hdrs = [
        "tmp_dir.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "async",
    hdrs = [
        "async.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":test_macros",
        "//src/v/base",
        "//src/v/model:timeout_clock",
        "//src/v/ssx:sformat",
        "@seastar",
    ],
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
        "//src/v/random:generators",
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
    name = "container_ostream",
    hdrs = [
        "container_ostream.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@fmt",
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
        ":test_macros",
        "//src/v/bytes:iostream",
        "//src/v/cloud_storage",
        "//src/v/cluster",
        "//src/v/model/tests:random",
        "//src/v/storage/tests:disk_log_builder",
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

# Gives access to test environment configuration such as TEST_TMPDIR.
redpanda_test_cc_library(
    name = "test_env",
    srcs = [
        "test_env.cc",
    ],
    hdrs = [
        "test_env.h",
    ],
    implementation_deps = [
        "//src/v/random:generators",
        "@abseil-cpp//absl/strings",
    ],
    visibility = ["//visibility:public"],
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

# Test hooks are things that run before/after test cases or the entire test
# run. This library has global test hooks which should run for every type of
# test (gtest or btest).
redpanda_test_cc_library(
    name = "global_test_hooks",
    srcs = [
        "global_test_hooks.cc",
    ],
    hdrs = [
        "global_test_hooks.h",
    ],
    implementation_deps = [
        "//src/v/random:generators",
        "//src/v/random:test_seeding",
        "//src/v/test_utils:test_env",
        "@fmt",
    ],
    visibility = ["//visibility:public"],
)

# The hook implementation for boost tests, installs the global test hooks.
redpanda_test_cc_library(
    name = "boost_test_hooks",
    srcs = [
        "boost_test_hooks.cc",
    ],
    hdrs = [
    ],
    implementation_deps = [
        ":global_test_hooks",
        "@boost//:test",
    ],
    visibility = ["//visibility:public"],
    deps = [
    ],
)

# The hook implementation for perf tests, installs the global test hooks.
redpanda_test_cc_library(
    name = "rpbench_hooks",
    srcs = [
        "rpbench_hooks.cc",
    ],
    implementation_deps = [
        ":global_test_hooks",
        "@seastar//:benchmark",
    ],
    visibility = ["//visibility:public"],
    alwayslink = True,  # hooks are installed by a global constructor
)

redpanda_test_cc_library(
    name = "metrics",
    hdrs = [
        "metrics.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "seastar_fuzz",
    srcs = [
        "seastar_fuzz.cc",
    ],
    hdrs = [
        "seastar_fuzz.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "@seastar",
        "@seastar//:testing",
    ],
    visibility = ["//visibility:public"],
)
