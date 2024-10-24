load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "dl_stm_state_test",
    timeout = "short",
    srcs = [
        "dl_stm_state_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/dl_stm:dl_stm_state",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/random:generators",
        "//src/v/serde",
        "//src/v/serde:enum",
        "//src/v/serde:map",
        "//src/v/serde:named_type",
        "//src/v/serde:uuid",
        "//src/v/serde:vector",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "//src/v/utils:uuid",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "dl_stm_test",
    timeout = "short",
    srcs = [
        "dl_stm_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/dl_stm",
        "//src/v/model",
        "//src/v/model/tests:random",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:random",
        "@seastar",
    ],
)
