load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "keys",
    srcs = [
        "keys.cc",
    ],
    hdrs = [
        "keys.h",
    ],
    implementation_deps = [
        "//src/v/bytes",
        "//src/v/utils:uuid",
        "@boost//:algorithm",
        "@seastar",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/model",
    ],
)

redpanda_cc_library(
    name = "write_batch_row",
    hdrs = [
        "write_batch_row.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/serde",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "state",
    srcs = [
        "state.cc",
    ],
    hdrs = [
        "state.h",
    ],
    implementation_deps = [
        "//src/v/serde:iobuf",
        "//src/v/serde:named_type",
        "//src/v/serde:optional",
        "//src/v/serde:sstring",
        "//src/v/serde:uuid",
        "//src/v/serde:vector",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":write_batch_row",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/cloud_topics/level_one/metastore:domain_uuid",
        "//src/v/container:chunked_vector",
        "//src/v/lsm",
        "//src/v/model",
        "//src/v/serde",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "lsm_update",
    srcs = [
        "lsm_update.cc",
    ],
    hdrs = [
        "lsm_update.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics/level_one/metastore:domain_uuid",
        "//src/v/serde:iobuf",
        "//src/v/serde:named_type",
        "//src/v/serde:optional",
        "//src/v/serde:uuid",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":state",
        ":write_batch_row",
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/utils:named_type",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "stm",
    srcs = [
        "stm.cc",
    ],
    hdrs = [
        "stm.h",
    ],
    implementation_deps = [
        "//src/v/cloud_topics:logger",
        "//src/v/serde",
        "//src/v/ssx:future_util",
        "//src/v/utils:prefix_logger",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":lsm_update",
        ":state",
        "//src/v/cluster:state_machine_registry",
        "//src/v/config",
        "//src/v/model",
        "//src/v/raft",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "state_reader",
    srcs = [
        "state_reader.cc",
    ],
    hdrs = [
        "state_reader.h",
    ],
    implementation_deps = [
        ":keys",
        "//src/v/lsm/core:exceptions",
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":values",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/lsm",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "values",
    hdrs = [
        "values.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore:state",
        "//src/v/model",
        "//src/v/serde",
    ],
)
