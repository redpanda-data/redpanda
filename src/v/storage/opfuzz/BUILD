load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "opfuzz",
    srcs = [
        "opfuzz.cc",
    ],
    hdrs = [
        "opfuzz.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/utils:directory_walker",
        "@boost//:algorithm",
    ],
    deps = [
        "//src/v/model",
        "//src/v/storage",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "opfuzz_test",
    timeout = "moderate",
    srcs = [
        "opfuzz_test.cc",
    ],
    cpu = 1,
    deps = [
        ":opfuzz",
        "//src/v/base",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/storage/tests:storage_test_fixture",
        "//src/v/test_utils:gtest",
        "@boost//:range",
        "@googletest//:gtest",
        "@seastar",
    ],
)
