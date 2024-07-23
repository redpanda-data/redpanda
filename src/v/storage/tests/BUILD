load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "disk_log_builder",
    srcs = [
        "utils/disk_log_builder.cc",
    ],
    hdrs = [
        "utils/disk_log_builder.h",
    ],
    include_prefix = "storage/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/features",
        "//src/v/model/tests:random",
        "//src/v/storage",
    ],
)

redpanda_cc_gtest(
    name = "segment_offset_tracker_test",
    timeout = "short",
    srcs = [
        "segment_offset_tracker_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/features",
        "//src/v/model",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_btest(
    name = "log_segment_appender_test",
    timeout = "short",
    srcs = [
        "log_segment_appender_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/bytes:random",
        "//src/v/config",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@fmt",
        "@seastar",
        "@seastar//:testing",
    ],
)
