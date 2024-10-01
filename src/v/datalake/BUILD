load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "conversion_outcome",
    srcs = [
    ],
    hdrs = [
        "conversion_outcome.h",
    ],
    include_prefix = "datalake",
    deps = [
        "@seastar",
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
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        ":conversion_outcome",
        "//src/v/iceberg:datatypes",
        "@fmt",
        "@protobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "logger",
    hdrs = [
        "logger.h",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "types",
    hdrs = [
        "errors.h",
        "fwd.h",
    ],
    include_prefix = "datalake",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "writer",
    hdrs = [
        "data_writer_interface.h",
        "schemaless_translator.h",
    ],
    include_prefix = "datalake",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/base",
        "//src/v/iceberg:datatypes",
        "//src/v/iceberg:values",
        "//src/v/serde",
    ],
)

redpanda_cc_library(
    name = "manager",
    srcs = [
        "datalake_manager.cc",
    ],
    hdrs = [
        "datalake_manager.h",
    ],
    include_prefix = "datalake",
    visibility = ["//visibility:public"],
    deps = [
        ":logger",
        ":types",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/datalake/coordinator:frontend",
        "//src/v/model",
        "//src/v/raft",
        "//src/v/serde",
        "//src/v/ssx:semaphore",
        "@fmt",
        "@seastar",
    ],
)
