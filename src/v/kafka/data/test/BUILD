load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "utils",
    srcs = [
        "utils.cc",
    ],
    hdrs = [
        "utils.h",
    ],
    implementation_deps = [
        "//src/v/random:generators",
    ],
    include_prefix = "kafka/data/test",
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
    ],
)

redpanda_cc_gtest(
    name = "record_batcher_test",
    timeout = "short",
    srcs = ["record_batcher_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [
        ":utils",
        "//src/v/base",
        "//src/v/kafka/data:record_batcher",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
