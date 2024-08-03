load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "utils",
    hdrs = [
        "utils.h",
    ],
    include_prefix = "cluster/tests",
    deps = [
    ],
)

redpanda_test_cc_library(
    name = "topic_table_fixture",
    hdrs = [
        "topic_table_fixture.h",
    ],
    include_prefix = "cluster/tests",
    deps = [
    ],
)

redpanda_cc_btest(
    name = "topic_table_test",
    timeout = "short",
    srcs = [
        "topic_table_test.cc",
    ],
    deps = [
        ":topic_table_fixture",
        ":utils",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
        "@seastar//:testing",
    ],
)
