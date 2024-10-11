load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "sorted_run_test",
    timeout = "short",
    srcs = [
        "sorted_run_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/dl_stm:sorted_run",
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
    name = "overlay_collection_test",
    timeout = "short",
    srcs = [
        "overlay_collection_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/cloud_topics:types",
        "//src/v/cloud_topics/dl_stm:overlay_collection",
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
