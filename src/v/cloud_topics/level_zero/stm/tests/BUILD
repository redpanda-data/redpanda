load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "size_estimator_test",
    timeout = "short",
    srcs = [
        "size_estimator_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_zero/stm:size_estimator",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "ctp_stm_state_test",
    timeout = "short",
    srcs = [
        "ctp_stm_state_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm_state",
        "//src/v/cloud_topics/level_zero/stm:dl_version",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "//src/v/utils:uuid",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "ctp_stm_test",
    timeout = "short",
    srcs = [
        "ctp_stm_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm_api",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm_commands",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm_factory",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm_state",
        "//src/v/cloud_topics/level_zero/stm:placeholder",
        "//src/v/cluster:state_machine_registry",
        "//src/v/model",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/ssx:when_all",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
