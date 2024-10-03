load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "datalake_translation_stm_test",
    timeout = "short",
    srcs = [
        "state_machine_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/datalake/translation:stm",
        "//src/v/raft/tests:raft_fixiture_retry_policy",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/raft/tests:stm_raft_fixture",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
        "@seastar",
    ],
)
