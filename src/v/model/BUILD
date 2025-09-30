load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "fips_config",
    hdrs = [
        "fips_config.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":model",
        "//src/v/config",
    ],
)

redpanda_cc_library(
    name = "model",
    srcs = [
        "adl_serde.cc",
        "async_adl_serde.cc",
        "model.cc",
        "record.cc",
        "record_batch_reader.cc",
        "record_utils.cc",
        "transform.cc",
        "validation.cc",
    ],
    hdrs = [
        "adl_serde.h",
        "async_adl_serde.h",
        "compression.h",
        "errc.h",
        "fundamental.h",
        "kitp.h",
        "ktp.h",
        "limits.h",
        "metadata.h",
        "namespace.h",
        "offset_interval.h",
        "record.h",
        "record_batch_reader.h",
        "record_batch_types.h",
        "record_utils.h",
        "timeout_clock.h",
        "timestamp.h",
        "transform.h",
        "validation.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/hashing:crc32c",
        "//src/v/reflection:adl",
        "//src/v/serde",
        "//src/v/serde:bool_class",
        "//src/v/serde:enum",
        "//src/v/serde:iobuf",
        "//src/v/serde:map",
        "//src/v/serde:named_type",
        "//src/v/serde:optional",
        "//src/v/serde:set",
        "//src/v/serde:sstring",
        "//src/v/serde:tristate",
        "//src/v/serde:uuid",
        "//src/v/serde:variant",
        "//src/v/serde:vector",
        "//src/v/ssx:sformat",
        "//src/v/strings:string_switch",
        "//src/v/utils:base64",
        "//src/v/utils:move_canary",
        "//src/v/utils:named_type",
        "//src/v/utils:to_string",
        "//src/v/utils:tristate",
        "//src/v/utils:unresolved_address",
        "//src/v/utils:uuid",
        "//src/v/utils:vint",
        "//src/v/utils:xid",
        "@abseil-cpp//absl/container:btree",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@abseil-cpp//absl/container:node_hash_map",
        "@abseil-cpp//absl/hash",
        "@abseil-cpp//absl/strings",
        "@boost//:container_hash",
        "@boost//:iterator",
        "@boost//:range",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "timeout_clock",
    hdrs = [
        "timeout_clock.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "batch_utils",
    srcs = [
        "batch_utils.cc",
    ],
    hdrs = [
        "batch_utils.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":model",
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "batch_builder",
    srcs = [
        "batch_builder.cc",
    ],
    hdrs = [
        "batch_builder.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":batch_compression",
        ":model",
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "batch_compression",
    srcs = [
        "batch_compression.cc",
    ],
    hdrs = [
        "batch_compression.h",
    ],
    implementation_deps = [
        "//src/v/compression",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":model",
        "//src/v/bytes:iobuf",
        "@seastar",
    ],
)
