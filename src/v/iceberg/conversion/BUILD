load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "conversion_outcome",
    srcs = [
    ],
    hdrs = [
        "conversion_outcome.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/iceberg:values",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "protobuf_utils",
    srcs = [
        "protobuf_utils.cc",
    ],
    hdrs = [
        "protobuf_utils.h",
    ],
    visibility = [":__subpackages__"],
    deps = [
        "@protobuf",
    ],
)

redpanda_cc_library(
    name = "avro_utils",
    srcs = [
        "avro_utils.cc",
    ],
    hdrs = [
        "avro_utils.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/iceberg:datatypes",
        "//src/v/ssx:sformat",
        "@avro",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "schema_avro",
    srcs = [
        "schema_avro.cc",
    ],
    hdrs = [
        "schema_avro.h",
    ],
    implementation_deps = [
        ":avro_utils",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":conversion_outcome",
        "//src/v/iceberg:datatypes",
        "@avro",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "schema_parquet",
    srcs = [
        "schema_parquet.cc",
    ],
    hdrs = [
        "schema_parquet.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/iceberg:datatypes",
        "//src/v/serde/parquet:schema",
    ],
)

redpanda_cc_library(
    name = "schema_protobuf",
    srcs = [
        "schema_protobuf.cc",
    ],
    hdrs = [
        "schema_protobuf.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":conversion_outcome",
        ":protobuf_utils",
        "//src/v/iceberg:datatypes",
        "@fmt",
        "@protobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "values_avro",
    srcs = ["values_avro.cc"],
    hdrs = ["values_avro.h"],
    implementation_deps = [
        ":avro_utils",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/iceberg:avro_decimal",
        "//src/v/serde/avro",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":conversion_outcome",
        "@avro",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "values_parquet",
    srcs = [
        "values_parquet.cc",
    ],
    hdrs = [
        "values_parquet.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":conversion_outcome",
        "//src/v/iceberg:values",
        "//src/v/serde/parquet:value",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "values_protobuf",
    srcs = [
        "values_protobuf.cc",
    ],
    hdrs = [
        "values_protobuf.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":conversion_outcome",
        ":protobuf_utils",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "//src/v/iceberg:values",
        "//src/v/serde/json:writer",
        "//src/v/serde/protobuf",
        "//src/v/ssx:future_util",
        "@abseil-cpp//absl/time",
        "@fmt",
        "@protobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "ir_json",
    srcs = [
        "ir_json.cc",
    ],
    hdrs = [
        "ir_json.h",
    ],
    implementation_deps = [
        "//src/v/base",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg/conversion/json_schema:frontend",
        "@fmt",
        "@seastar",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":conversion_outcome",
        "//src/v/iceberg/conversion/json_schema:ir",
    ],
)

redpanda_cc_library(
    name = "schema_json",
    srcs = [
        "schema_json.cc",
    ],
    hdrs = [
        "schema_json.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":conversion_outcome",
        ":ir_json",
        "//src/v/iceberg:datatypes",
    ],
)

redpanda_cc_library(
    name = "values_json",
    srcs = [
        "values_json.cc",
    ],
    hdrs = [
        "values_json.h",
    ],
    implementation_deps = [
        ":time_rfc3339",
        "//src/v/bytes:iobuf_parser",
        "//src/v/serde/json:parser",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":conversion_outcome",
        ":ir_json",
        "//src/v/bytes:iobuf",
        "//src/v/iceberg:values",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "time_rfc3339",
    srcs = [
        "time_rfc3339.cc",
    ],
    hdrs = [
        "time_rfc3339.h",
    ],
    implementation_deps = [
        "@abseil-cpp//absl/time",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":conversion_outcome",
        "//src/v/iceberg:values",
        "@seastar",
    ],
)
