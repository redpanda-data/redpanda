load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_cc_gtest(
    name = "state_machine_test",
    timeout = "short",
    srcs = [
        "state_machine_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/datalake/translation:stm",
        "//src/v/datalake/translation:utils",
        "//src/v/raft/tests:stm_test_fixture",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_test_cc_library(
    name = "scheduler_fixture",
    srcs = [
        "scheduler_fixture.cc",
    ],
    hdrs = [
        "scheduler_fixture.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/datalake:logger",
        "//src/v/datalake/translation:scheduler",
        "//src/v/random:generators",
        "//src/v/ssx:future_util",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "scheduler_test",
    timeout = "moderate",
    srcs = [
        "scheduler_tests.cc",
    ],
    cpu = 1,
    deps = [
        ":scheduler_fixture",
        "//src/v/datalake:logger",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "fair_scheduler_test",
    timeout = "moderate",
    srcs = [
        "fair_scheduling_policy_tests.cc",
    ],
    cpu = 1,
    deps = [
        ":scheduler_fixture",
        "//src/v/datalake:logger",
        "//src/v/datalake/translation:scheduler",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "partition_translator_test",
    timeout = "short",
    srcs = [
        "partition_translator_tests.cc",
    ],
    cpu = 1,
    deps = [
        ":scheduler_fixture",
        "//src/v/datalake/translation:partition_translator",
        "//src/v/datalake/translation:translator_deps",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "translated_log_offset_test",
    timeout = "short",
    srcs = [
        "translated_log_offset_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/datalake/translation:model",
        "//src/v/datalake/translation:utils",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/storage",
        "//src/v/storage/tests:disk_log_builder",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@googletest//:gtest",
        "@seastar",
    ],
)
