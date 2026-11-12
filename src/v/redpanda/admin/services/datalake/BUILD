load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "datalake",
    srcs = ["datalake.cc"],
    hdrs = ["datalake.h"],
    implementation_deps = [
        "//src/v/datalake:schema_migration_detector",
        "//src/v/datalake:table_id_provider",
        "//src/v/iceberg:rename_columns_action",
        "//src/v/iceberg:transaction",
    ],
    visibility = ["//src/v/redpanda:__subpackages__"],
    deps = [
        "//proto/redpanda/core/admin/internal/datalake/v1:datalake_redpanda_proto",
        "//src/v/cluster:fwd",
        "//src/v/container:chunked_hash_map",
        "//src/v/datalake/coordinator:coordinator_manager",
        "//src/v/datalake/coordinator:frontend",
        "//src/v/datalake/coordinator:model",
        "//src/v/datalake/coordinator:state",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "@seastar",
    ],
)
