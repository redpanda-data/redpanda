load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "dl_stm_test",
    timeout = "short",
    srcs = [
        "dl_stm_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/dl_stm",
        "//src/v/cloud_topics/dl_stm:dl_stm_factory",
        "//src/v/raft/tests:raft_fixture",
    ],
)
