load("//bazel:build.bzl", "redpanda_cc_binary")
load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "limiter_test",
    timeout = "short",
    srcs = [
        "limiter_test.cc",
    ],
    deps = [
        "//src/v/crash_tracker",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "//src/v/utils:arch",
        "//src/v/version",
        "@fmt",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "recorder_test",
    timeout = "short",
    srcs = [
        "recorder_test.cc",
        "utils.h",
    ],
    deps = [
        "//src/v/config",
        "//src/v/crash_tracker",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "types_test",
    timeout = "short",
    srcs = [
        "types_test.cc",
    ],
    deps = [
        "//src/v/crash_tracker",
        "//src/v/test_utils:gtest",
        "@fmt",
        "@googletest//:gtest",
    ],
)

redpanda_cc_binary(
    name = "crash_report_generator",
    testonly = True,
    srcs = ["report_generator.cc"],
    deps = [
        "//src/v/base",
        "//src/v/crash_tracker",
        "//src/v/model",
        "//src/v/utils:file_io",
        "@boost//:program_options",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "crash_reporter_oom_test",
    timeout = "short",
    srcs = [
        "crash_reporter_oom_test.cc",
        "utils.h",
    ],
    deps = [
        "//src/v/config",
        "//src/v/crash_tracker",
        "//src/v/resource_mgmt:memory_sampling",
        "//src/v/test_utils:gtest",
        "//src/v/utils:human",
        "@googletest//:gtest",
        "@seastar",
    ],
)
