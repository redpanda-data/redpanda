load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_gtest")

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
