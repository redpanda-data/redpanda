load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "state_machine_test",
    timeout = "short",
    srcs = [
        "state_machine_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/datalake/translation:stm",
        "//src/v/raft/tests:stm_test_fixture",
        "//src/v/random:generators",
        "//src/v/storage",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
        "@seastar",
    ],
)
