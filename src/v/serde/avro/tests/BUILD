load("//bazel:test.bzl", "redpanda_cc_gtest", "redpanda_test_cc_library")
load("//bazel/thirdparty:avro.bzl", "avro_cc_library")

avro_cc_library(
    name = "avro_test_array",
    class_name = "avro_test_array",
    include_prefix = "serde/avro/tests",
    namespace = "serde::avro::tests",
    schema = "testdata/array",
)

redpanda_test_cc_library(
    name = "data_generator",
    srcs = [
        "data_generator.cc",
    ],
    hdrs = [
        "data_generator.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/random:generators",
    ],
    include_prefix = "serde/avro/tests",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/utils:uuid",
        "@avro",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "parser_test",
    timeout = "short",
    srcs = [
        "parser_test.cc",
    ],
    cpu = 1,
    data = [
        "testdata/array",
        "testdata/big_union",
        "testdata/bigrecord",
        "testdata/bigrecord2",
        "testdata/bigrecord_r",
        "testdata/circulardep",
        "testdata/cpp_reserved_words",
        "testdata/cpp_reserved_words_union_typedef",
        "testdata/crossref",
        "testdata/empty_record",
        "testdata/enum",
        "testdata/fixed",
        "testdata/int",
        "testdata/large_schema.avsc",
        "testdata/map",
        "testdata/padded_record",
        "testdata/primitivetypes",
        "testdata/recinrec",
        "testdata/record",
        "testdata/record2",
        "testdata/reuse",
        "testdata/tree1",
        "testdata/tree2",
        "testdata/tweet",
        "testdata/union",
        "testdata/union_array_union",
        "testdata/union_conflict",
        "testdata/union_empty_record",
        "testdata/union_map_union",
        "testdata/union_redundant_types",
        "testdata/unionwithmap",
    ],
    memory = "128MiB",
    deps = [
        ":avro_test_array",
        ":data_generator",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/bytes:random",
        "//src/v/random:generators",
        "//src/v/serde/avro",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:runfiles",
        "//src/v/utils:file_io",
        "@avro",
        "@fmt",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
        "@seastar",
    ],
)
