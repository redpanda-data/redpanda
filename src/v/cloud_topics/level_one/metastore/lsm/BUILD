load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "debug_serde",
    srcs = [
        "debug_serde.cc",
    ],
    hdrs = [
        "debug_serde.h",
    ],
    implementation_deps = [
        ":keys",
        ":values",
        "//src/v/cloud_topics/level_one/metastore:state",
        "//src/v/serde",
        "//src/v/utils:uuid",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//proto/redpanda/core/admin/internal/cloud_topics/v1:metastore_redpanda_proto",
        "//src/v/utils:detailed_error",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "debug_reader",
    srcs = [
        "debug_reader.cc",
    ],
    hdrs = [
        "debug_reader.h",
    ],
    implementation_deps = [
        ":values",
        "//src/v/cloud_topics/level_one/metastore:state",
        "//src/v/serde",
        "//src/v/strings:string_switch",
        "//src/v/utils:uuid",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":debug_serde",
        ":keys",
        ":write_batch_row",
        "//src/v/container:chunked_vector",
    ],
)

redpanda_cc_library(
    name = "debug_writer",
    srcs = [
        "debug_writer.cc",
    ],
    hdrs = [
        "debug_writer.h",
    ],
    implementation_deps = [
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":debug_serde",
        ":write_batch_row",
        "//src/v/container:chunked_vector",
    ],
)

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
        "//src/v/cloud_topics:logger",
        "//src/v/lsm/core:exceptions",
        "//src/v/ssx:future_util",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":values",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/lsm",
        "//src/v/model",
        "//src/v/utils:detailed_error",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "replicated_db",
    srcs = [
        "replicated_db.cc",
    ],
    hdrs = [
        "replicated_db.h",
    ],
    implementation_deps = [
        ":replicated_persistence",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/metastore:domain_uuid",
        "//src/v/config",
        "//src/v/lsm/io:cloud_persistence",
        "//src/v/model:batch_builder",
        "//src/v/serde",
        "//src/v/ssx:clock",
        "//src/v/ssx:time",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":lsm_update",
        ":stm",
        ":write_batch_row",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
        "//src/v/container:chunked_vector",
        "//src/v/lsm",
        "//src/v/lsm/io:persistence",
        "//src/v/lsm/proto:manifest_redpanda_proto",
        "//src/v/model",
        "//src/v/ssx:future_util",
        "//src/v/utils:detailed_error",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "replicated_persistence",
    srcs = [
        "replicated_persistence.cc",
    ],
    hdrs = [
        "replicated_persistence.h",
    ],
    implementation_deps = [
        "//src/v/lsm",
        "//src/v/lsm/io:cloud_persistence",
        "//src/v/model:batch_builder",
        "//src/v/serde",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":lsm_update",
        ":stm",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage_clients",
        "//src/v/lsm/core:exceptions",
        "//src/v/lsm/io:persistence",
        "//src/v/lsm/proto:manifest_redpanda_proto",
        "//src/v/model",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "state_update",
    srcs = ["state_update.cc"],
    hdrs = ["state_update.h"],
    implementation_deps = [
        ":keys",
        ":values",
        "//src/v/cloud_topics/level_one/metastore:state_update_utils",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":state_reader",
        ":write_batch_row",
        "//src/v/base",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/cloud_topics/level_one/metastore:state_update",
        "//src/v/utils:named_type",
        "@abseil-cpp//absl/container:btree",
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

redpanda_cc_library(
    name = "garbage_collector",
    srcs = [
        "garbage_collector.cc",
    ],
    hdrs = [
        "garbage_collector.h",
    ],
    implementation_deps = [
        ":keys",
        ":state_reader",
        ":state_update",
        "//src/v/base",
        "//src/v/cloud_topics:logger",
        "//src/v/cloud_topics/level_one/common:abstract_io",
        "//src/v/cloud_topics/level_one/domain:domain_manager_probe",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":replicated_db",
        "//src/v/base",
        "//src/v/cloud_topics/level_one/common:object_id",
        "//src/v/container:chunked_vector",
        "//src/v/model",
        "//src/v/utils:detailed_error",
        "@seastar",
    ],
)
