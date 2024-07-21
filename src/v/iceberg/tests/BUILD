load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")

redpanda_test_cc_library(
    name = "test_schemas",
    srcs = [
        "test_schemas.cc",
    ],
    hdrs = [
        "test_schemas.h",
    ],
    include_prefix = "iceberg/tests",
    deps = [
        "//src/v/iceberg:datatypes",
    ],
)

redpanda_cc_gtest(
    name = "datatypes_test",
    timeout = "short",
    srcs = [
        "datatypes_test.cc",
    ],
    deps = [
        "//src/v/iceberg:datatypes",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "datatypes_json_test",
    timeout = "short",
    srcs = [
        "datatypes_json_test.cc",
    ],
    deps = [
        ":test_schemas",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:datatypes_json",
        "//src/v/json",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "manifest_serialization_test",
    timeout = "short",
    srcs = [
        "manifest_serialization_test.cc",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/iceberg:avro_utils",
        "//src/v/iceberg:manifest_entry",
        "//src/v/iceberg:manifest_file",
        "//src/v/test_utils:gtest",
        "@avro",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "schema_json_test",
    timeout = "short",
    srcs = [
        "schema_json_test.cc",
    ],
    deps = [
        ":test_schemas",
        "//src/v/iceberg:schema",
        "//src/v/iceberg:schema_json",
        "//src/v/json",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
