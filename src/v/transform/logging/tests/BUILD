load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "utils",
    srcs = ["utils.cc"],
    hdrs = ["utils.h"],
    implementation_deps = [
        "//src/v/random:generators",
    ],
    include_prefix = "transform/logging/tests",
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/json",
        "//src/v/model",
    ],
)

redpanda_cc_gtest(
    name = "model_test",
    timeout = "short",
    srcs = ["model_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [
        ":utils",
        "//src/v/json",
        "//src/v/test_utils:gtest",
        "//src/v/transform/logging:event",
        "@googletest//:gtest",
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
        "//src/v/test_utils:gtest",
        "//src/v/transform/logging:record_batcher",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "log_manager_test",
    timeout = "short",
    srcs = ["log_manager_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [
        ":utils",
        "//src/v/base",
        "//src/v/config",
        "//src/v/model",
        "//src/v/strings:utf8",
        "//src/v/test_utils:gtest",
        "//src/v/transform/logging",
        "//src/v/transform/logging:event",
        "@abseil-cpp//absl/strings",
        "@googletest//:gtest",
        "@seastar",
    ],
)
