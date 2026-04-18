load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "utils",
    srcs = [
        "utils.cc",
    ],
    hdrs = [
        "utils.h",
    ],
    implementation_deps = [
        "//src/v/random:generators",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
    ],
)

redpanda_cc_gtest(
    name = "encrypting_proxy_test",
    timeout = "short",
    srcs = ["encrypting_proxy_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/cluster",
        "//src/v/encryption",
        "//src/v/encryption:encryption_metadata_ser",
        "//src/v/encryption:field_transformer",
        "//src/v/encryption:schema_resolver",
        "//src/v/kafka/data:encrypting_partition_proxy",
        "//src/v/kafka/data:partition_proxy",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "record_batcher_test",
    timeout = "short",
    srcs = ["record_batcher_test.cc"],
    cpu = 1,
    memory = "128MiB",
    deps = [
        ":utils",
        "//src/v/base",
        "//src/v/kafka/data:record_batcher",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
