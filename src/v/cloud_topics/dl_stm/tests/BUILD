load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "dl_stm_state_test",
    timeout = "short",
    srcs = [
        "dl_stm_state_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics:dl_overlay",
        "//src/v/cloud_topics:dl_version",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/dl_stm:dl_stm_state",
        "//src/v/model",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "//src/v/utils:uuid",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "dl_stm_test",
    timeout = "short",
    srcs = [
        "dl_stm_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics:dl_overlay",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/dl_stm",
        "//src/v/cloud_topics/dl_stm:dl_stm_api",
        "//src/v/cloud_topics/dl_stm:dl_stm_factory",
        "//src/v/cloud_topics/dl_stm:dl_stm_state",
        "//src/v/model",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
