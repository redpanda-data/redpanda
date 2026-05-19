load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "datalake",
    srcs = ["datalake.cc"],
    hdrs = ["datalake.h"],
    visibility = ["//src/v/redpanda:__subpackages__"],
    deps = [
        "//proto/redpanda/core/admin/internal/datalake/v1:datalake_redpanda_proto",
        "//src/v/cloud_io:remote",
        "//src/v/cloud_storage",
        "//src/v/cloud_storage_clients",
        "//src/v/cluster:fwd",
        "//src/v/config",
        "//src/v/container:chunked_hash_map",
        "//src/v/datalake:credential_manager",
        "//src/v/datalake/coordinator:catalog_factory",
        "//src/v/datalake/coordinator:frontend",
        "//src/v/datalake/coordinator:model",
        "//src/v/datalake/coordinator:state",
        "//src/v/iceberg:catalog",
        "//src/v/iceberg:catalog_errors",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "@seastar",
        "@yaml-cpp",
    ],
)
