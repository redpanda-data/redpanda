load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "encryption_service",
    srcs = [
        "encryption_service.cc",
    ],
    hdrs = [
        "encryption_service.h",
    ],
    implementation_deps = [
        ":field_transformer",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":encryption",
        ":schema_resolver",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "encryption",
    srcs = [
        "dek_manager.cc",
        "mock_kms_provider.cc",
    ],
    hdrs = [
        "dek_manager.h",
        "encryption_services.h",
        "fwd.h",
        "kms_provider.h",
        "mock_kms_provider.h",
        "types.h",
    ],
    implementation_deps = [
        "@openssl",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/container:chunked_hash_map",
        "//src/v/crypto",
        "//src/v/model",
        "@abseil-cpp//absl/hash",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "encryption_metadata_ser",
    srcs = [
        "encryption_metadata_ser.cc",
    ],
    hdrs = [
        "encryption_metadata_ser.h",
    ],
    implementation_deps = [
        "//proto/redpanda/core/encryption/v1:encryption_redpanda_proto",
        "//src/v/storage:record_batch_builder",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":encryption",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "dek_dedup",
    srcs = [
        "dek_dedup.cc",
    ],
    hdrs = [
        "dek_dedup.h",
    ],
    implementation_deps = [
        ":encryption_metadata_ser",
        "//src/v/storage:record_batch_builder",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":encryption",
        "//src/v/base",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "dek_refill",
    srcs = [
        "dek_refill.cc",
    ],
    hdrs = [
        "dek_refill.h",
    ],
    implementation_deps = [
        ":encryption_metadata_ser",
        "//src/v/storage:record_batch_builder",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":encryption",
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "schema_annotation_parser",
    srcs = [
        "schema_annotation_parser.cc",
    ],
    hdrs = [
        "schema_annotation_parser.h",
    ],
    implementation_deps = [
        "//src/v/json",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "schema_resolver",
    srcs = [
        "schema_resolver.cc",
    ],
    hdrs = [
        "schema_resolver.h",
    ],
    implementation_deps = [
        ":schema_annotation_parser",
        "//src/v/base",
        "@avro",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":field_transformer",
        "//src/v/container:chunked_hash_map",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "field_transformer",
    srcs = [
        "field_transformer_ref.cc",
    ],
    hdrs = [
        "crypto_utils.h",
        "field_transformer.h",
        "field_transformer_ref.h",
    ],
    implementation_deps = [
        "//src/v/bytes:iobuf_parser",
        "//src/v/serde/avro",
        "//src/v/serde/json:parser",
        "//src/v/serde/json:writer",
        "//src/v/serde/protobuf",
        "//src/v/serde/protobuf:encoder",
        "//src/v/utils:base64",
        "@openssl",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":encryption",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/crypto",
        "@avro",
        "@fmt",
        "@protobuf",
        "@seastar",
    ],
)
