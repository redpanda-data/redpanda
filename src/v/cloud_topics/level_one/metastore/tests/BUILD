load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "simple_metastore_test",
    timeout = "short",
    srcs = [
        "simple_metastore_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore:simple",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "replicated_metastore_test",
    timeout = "short",
    srcs = [
        "replicated_metastore_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_topics:app",
        "//src/v/cloud_topics/level_one/metastore:replicated_metastore",
        "//src/v/cloud_topics/tests:cluster_fixture",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "state_test",
    timeout = "short",
    srcs = [
        "state_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics/level_one/metastore:state",
        "//src/v/serde",
        "//src/v/serde:named_type",
        "//src/v/serde:uuid",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "state_update_test",
    timeout = "short",
    srcs = [
        "state_update_test.cc",
    ],
    deps = [
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore:state",
        "//src/v/cloud_topics/level_one/metastore:state_update",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "//src/v/utils:uuid",
        "@abseil-cpp//absl/strings",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "offset_interval_set_test",
    timeout = "short",
    srcs = [
        "offset_interval_set_test.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/cloud_topics/level_one/metastore:offset_interval_set",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
