load("//bazel:test.bzl", "redpanda_cc_btest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "utils",
    hdrs = [
        "utils.h",
    ],
    include_prefix = "kafka/client/test",
    deps = [
        "//src/v/reflection:adl",
        "//src/v/storage:record_batch_builder",
    ],
)

redpanda_cc_btest(
    name = "partitioners_test",
    timeout = "short",
    srcs = [
        "partitioners.cc",
    ],
    deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/hashing:murmur",
        "//src/v/kafka/client",
        "//src/v/model",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "fetch_session_test",
    timeout = "short",
    srcs = [
        "fetch_session.cc",
    ],
    deps = [
        ":utils",
        "//src/v/kafka/client",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:fetch",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "produce_batcher_test",
    timeout = "short",
    srcs = [
        "produce_batcher.cc",
    ],
    deps = [
        ":utils",
        "//src/v/cluster",
        "//src/v/kafka/client",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:produce",
        "//src/v/model",
        "//src/v/reflection:adl",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:seastar_boost",
        "@seastar",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "produce_partition_test",
    timeout = "short",
    srcs = [
        "produce_partition.cc",
    ],
    deps = [
        ":utils",
        "//src/v/kafka/client",
        "//src/v/kafka/protocol",
        "//src/v/kafka/protocol:produce",
        "//src/v/model",
        "//src/v/test_utils:seastar_boost",
        "@boost//:test",
        "@seastar//:testing",
    ],
)

redpanda_cc_btest(
    name = "retry_with_mitigation_test",
    timeout = "short",
    srcs = [
        "retry_with_mitigation.cc",
    ],
    deps = [
        "//src/v/kafka/client",
        "//src/v/test_utils:seastar_boost",
        "@abseil-cpp//absl/container:flat_hash_set",
        "@boost//:test",
        "@seastar",
        "@seastar//:testing",
    ],
)
