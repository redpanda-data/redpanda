load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "scoped_remote",
    srcs = [
        "scoped_remote.cc",
    ],
    hdrs = [
        "scoped_remote.h",
    ],
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
        "@boost//:algorithm",
        "@boost//:core",
        "@boost//:property_tree",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cloud_storage_clients",
        "//src/v/config",
        "//src/v/http:utils",
        "//src/v/http/tests:utils",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "cloud_io_cache_fixture",
    hdrs = [
        "cache_test_fixture.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_io:cache",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:tmp_dir",
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
        "//src/v/cloud_io:cache",
        "//src/v/config",
        "//src/v/random:generators",
        "//src/v/storage:disk",
        "//src/v/test_utils:iostream",
        "//src/v/test_utils:scoped_config",
        "//src/v/test_utils:seastar_boost",
        "//src/v/utils:directory_walker",
        "//src/v/utils:file_io",
        "//src/v/utils:human",
        "@boost//:filesystem",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "cache_rpbench",
    srcs = [
        "cache_bench.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/cloud_io:cache",
        "//src/v/config",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/ssx:sformat",
        "@abseil-cpp//absl/container:btree",
        "@seastar",
        "@seastar//:benchmark",
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
        "//src/v/cloud_io:cache",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)
