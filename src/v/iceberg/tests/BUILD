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

redpanda_test_cc_library(
    name = "value_generator",
    srcs = [
        "value_generator.cc",
    ],
    hdrs = [
        "value_generator.h",
    ],
    include_prefix = "iceberg/tests",
    deps = [
        "//src/v/bytes:random",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:values",
        "//src/v/random:generators",
        "//src/v/test_utils:random",
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
        ":test_schemas",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:fragmented_vector",
        "//src/v/iceberg:avro_utils",
        "//src/v/iceberg:manifest",
        "//src/v/iceberg:manifest_avro",
        "//src/v/iceberg:manifest_entry_gen",
        "//src/v/iceberg:manifest_file_gen",
        "//src/v/iceberg:schema_json",
        "//src/v/test_utils:gtest",
        "//src/v/utils:file_io",
        "@avro",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "partition_test",
    timeout = "short",
    srcs = [
        "partition_test.cc",
    ],
    deps = [
        "//src/v/container:fragmented_vector",
        "//src/v/iceberg:partition",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "partition_key_test",
    timeout = "short",
    srcs = [
        "partition_key_test.cc",
    ],
    deps = [
        ":test_schemas",
        ":value_generator",
        "//src/v/iceberg:partition",
        "//src/v/iceberg:partition_key",
        "//src/v/iceberg:struct_accessor",
        "//src/v/iceberg:transform",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "partition_key_type_test",
    timeout = "short",
    srcs = [
        "partition_key_type_test.cc",
    ],
    deps = [
        ":test_schemas",
        "//src/v/iceberg:partition",
        "//src/v/iceberg:partition_key_type",
        "//src/v/iceberg:schema",
        "//src/v/iceberg:transform",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "schema_avro_test",
    timeout = "short",
    srcs = [
        "schema_avro_test.cc",
    ],
    deps = [
        ":test_schemas",
        "//src/v/iceberg:schema",
        "//src/v/iceberg:schema_avro",
        "//src/v/test_utils:gtest",
        "@avro",
        "@googletest//:gtest",
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

redpanda_cc_gtest(
    name = "schema_test",
    timeout = "short",
    srcs = [
        "schema_test.cc",
    ],
    deps = [
        ":test_schemas",
        "//src/v/iceberg:schema",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "struct_accessor_test",
    timeout = "short",
    srcs = [
        "struct_accessor_test.cc",
    ],
    deps = [
        ":test_schemas",
        ":value_generator",
        "//src/v/iceberg:struct_accessor",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "transform_utils_test",
    timeout = "short",
    srcs = [
        "transform_utils_test.cc",
    ],
    deps = [
        "//src/v/iceberg:transform",
        "//src/v/iceberg:transform_utils",
        "//src/v/iceberg:values",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "values_avro_test",
    timeout = "short",
    srcs = [
        "values_avro_test.cc",
    ],
    deps = [
        ":test_schemas",
        ":value_generator",
        "//src/v/iceberg:schema_avro",
        "//src/v/iceberg:values",
        "//src/v/iceberg:values_avro",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)

redpanda_cc_gtest(
    name = "values_test",
    timeout = "short",
    srcs = [
        "values_test.cc",
    ],
    deps = [
        ":test_schemas",
        ":value_generator",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:fragmented_vector",
        "//src/v/iceberg:values",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
    ],
)
