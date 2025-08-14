load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "frontend_test",
    timeout = "short",
    srcs = [
        "frontend_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_topics:app",
        "//src/v/cloud_topics:data_plane_api",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/frontend",
        "//src/v/cloud_topics/level_zero/common:extent_meta",
        "//src/v/cloud_topics/level_zero/stm:ctp_stm",
        "//src/v/config",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft",
        "//src/v/random:generators",
        "//src/v/redpanda/tests:fixture",
        "//src/v/ssx:sformat",
        "//src/v/storage",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
        "@seastar",
        "@seastar//:testing",
    ],
)
