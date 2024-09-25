load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "schema_protobuf_test",
    timeout = "short",
    srcs = [
        "schema_protobuf_test.cc",
    ],
    data = [
        "testdata/iceberg_ready_test_messages_edition2023.proto",
    ],
    deps = [
        "//src/v/datalake:schema_protobuf",
        "//src/v/iceberg:datatypes",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:runfiles",
        "//src/v/utils:file_io",
        "@fmt",
        "@googletest//:gtest",
        "@protobuf",
        "@protobuf//src/google/protobuf/compiler:importer",
        "@protobuf//src/google/protobuf/io:tokenizer",
        "@seastar",
    ],
)
