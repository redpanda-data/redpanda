load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "encryption_service_test",
    timeout = "short",
    srcs = [
        "encryption_service_test.cc",
    ],
    deps = [
        "//src/v/encryption",
        "//src/v/encryption:encryption_service",
        "//src/v/encryption:schema_resolver",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "mock_kms_test",
    timeout = "short",
    srcs = [
        "mock_kms_test.cc",
    ],
    deps = [
        "//src/v/encryption",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "dek_manager_test",
    timeout = "short",
    srcs = [
        "dek_manager_test.cc",
    ],
    deps = [
        "//src/v/encryption",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "dek_dedup_test",
    timeout = "short",
    srcs = [
        "dek_dedup_test.cc",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/encryption",
        "//src/v/encryption:dek_dedup",
        "//src/v/encryption:encryption_metadata_ser",
        "//src/v/model",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "dek_refill_test",
    timeout = "short",
    srcs = [
        "dek_refill_test.cc",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/encryption",
        "//src/v/encryption:dek_refill",
        "//src/v/encryption:encryption_metadata_ser",
        "//src/v/model",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "encryption_metadata_ser_test",
    timeout = "short",
    srcs = [
        "encryption_metadata_ser_test.cc",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/encryption",
        "//src/v/encryption:encryption_metadata_ser",
        "//src/v/model",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "schema_annotation_parser_test",
    timeout = "short",
    srcs = [
        "schema_annotation_parser_test.cc",
    ],
    deps = [
        "//src/v/encryption:schema_annotation_parser",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "schema_resolver_test",
    timeout = "short",
    srcs = [
        "schema_resolver_test.cc",
    ],
    deps = [
        "//src/v/encryption:schema_resolver",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "field_transformer_test",
    timeout = "short",
    srcs = [
        "field_transformer_test.cc",
    ],
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/encryption",
        "//src/v/encryption:field_transformer",
        "//src/v/serde/avro",
        "//src/v/serde/json:parser",
        "//src/v/serde/protobuf",
        "//src/v/serde/protobuf:encoder",
        "//src/v/test_utils:gtest",
        "//src/v/utils:base64",
        "@avro",
        "@googletest//:gtest",
        "@protobuf",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "encryption_integration_test",
    timeout = "short",
    srcs = [
        "encryption_integration_test.cc",
    ],
    cpu = 1,
    memory = "128MiB",
    deps = [
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/cluster",
        "//src/v/encryption",
        "//src/v/encryption:dek_refill",
        "//src/v/encryption:encryption_metadata_ser",
        "//src/v/encryption:field_transformer",
        "//src/v/encryption:schema_resolver",
        "//src/v/kafka/data:encrypting_partition_proxy",
        "//src/v/kafka/data:partition_proxy",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/serde/avro",
        "//src/v/serde/json:parser",
        "//src/v/serde/protobuf",
        "//src/v/storage",
        "//src/v/storage:record_batch_builder",
        "//src/v/test_utils:gtest",
        "//src/v/utils:base64",
        "@avro",
        "@googletest//:gtest",
        "@protobuf",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "encrypt_bench_rpbench",
    srcs = ["encrypt_bench.cc"],
    deps = [
        "//src/v/encryption",
        "//src/v/encryption:field_transformer",
        "@avro",
        "@fmt",
        "@protobuf",
        "@seastar",
        "@seastar//:benchmark",
    ],
)
