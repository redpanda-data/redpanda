load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "encryption",
    srcs = [
        "dek_manager.cc",
        "mock_kms_provider.cc",
    ],
    hdrs = [
        "dek_manager.h",
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
