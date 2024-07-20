load("//bazel:build.bzl", "redpanda_cc_library")
load("//src/v/rpc:compiler.bzl", "redpanda_cc_rpc_library")

redpanda_cc_rpc_library(
    name = "bootstrap_rpc",
    src = "cluster_bootstrap.json",
)

redpanda_cc_rpc_library(
    name = "controller_rpc",
    src = "controller.json",
)

redpanda_cc_rpc_library(
    name = "metadata_rpc",
    src = "metadata_dissemination_rpc.json",
)

redpanda_cc_rpc_library(
    name = "id_allocator_rpc",
    src = "id_allocator.json",
)

redpanda_cc_rpc_library(
    name = "tx_gateway_rpc",
    src = "tx_gateway.json",
)

redpanda_cc_rpc_library(
    name = "partition_balancer_rpc",
    src = "partition_balancer_rpc.json",
)

redpanda_cc_rpc_library(
    name = "node_status_rpc",
    src = "node_status_rpc.json",
)

redpanda_cc_rpc_library(
    name = "ephemeral_credential_rpc",
    src = "ephemeral_credential_rpc.json",
)

redpanda_cc_rpc_library(
    name = "self_test_rpc",
    src = "self_test_rpc.json",
)

redpanda_cc_rpc_library(
    name = "topic_recovery_status_rpc",
    src = "topic_recovery_status_rpc.json",
)

redpanda_cc_rpc_library(
    name = "offsets_recovery_rpc",
    src = "offsets_recovery_rpc.json",
)

redpanda_cc_rpc_library(
    name = "tx_manager_migrator_rpc",
    src = "migrations/tx_manager_migrator.json",
    out = "tx_manager_migrator_service.h",
)

redpanda_cc_library(
    name = "version",
    hdrs = [
        "version.h",
    ],
    include_prefix = "cluster",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/utils:named_type",
    ],
)

redpanda_cc_library(
    name = "features",
    hdrs = [
        "feature_update_action.h",
    ],
    include_prefix = "cluster",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/serde",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "notification",
    hdrs = [
        "notification.h",
    ],
    include_prefix = "cluster",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/utils:named_type",
    ],
)
