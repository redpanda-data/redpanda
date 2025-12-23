load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "lsm_update_test",
    timeout = "short",
    srcs = [
        "lsm_update_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/metastore/lsm:lsm_update",
        "//src/v/cloud_topics/level_one/metastore/lsm:state",
        "//src/v/cloud_topics/level_one/metastore/lsm:write_batch_row",
        "//src/v/container:chunked_vector",
    ],
)

redpanda_cc_gtest(
    name = "keys_test",
    timeout = "short",
    srcs = [
        "keys_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore/lsm:keys",
        "//src/v/kafka/protocol",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
