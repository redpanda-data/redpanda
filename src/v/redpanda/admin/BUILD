load("//bazel/thirdparty:seastar.bzl", "seastar_cc_swagger_library")

seastar_cc_swagger_library(
    name = "broker_swagger",
    src = "api-doc/broker.json",
)

seastar_cc_swagger_library(
    name = "cluster",
    src = "api-doc/cluster.json",
)

seastar_cc_swagger_library(
    name = "cluster_config_swagger",
    src = "api-doc/cluster_config.json",
)

seastar_cc_swagger_library(
    name = "config_swagger",
    src = "api-doc/config.json",
    definitions = [
        "api-doc/config.def.json",
    ],
)

seastar_cc_swagger_library(
    name = "debug",
    src = "api-doc/debug.json",
)

seastar_cc_swagger_library(
    name = "features",
    src = "api-doc/features.json",
)

seastar_cc_swagger_library(
    name = "hbadger",
    src = "api-doc/hbadger.json",
)

seastar_cc_swagger_library(
    name = "partition",
    src = "api-doc/partition.json",
)

seastar_cc_swagger_library(
    name = "raft",
    src = "api-doc/raft.json",
)

seastar_cc_swagger_library(
    name = "recovery",
    src = "api-doc/recovery.json",
)

seastar_cc_swagger_library(
    name = "security_swagger",
    src = "api-doc/security.json",
    definitions = [
        "api-doc/security.def.json",
    ],
)

seastar_cc_swagger_library(
    name = "shadow_indexing",
    src = "api-doc/shadow_indexing.json",
)

seastar_cc_swagger_library(
    name = "status",
    src = "api-doc/status.json",
)

seastar_cc_swagger_library(
    name = "transaction",
    src = "api-doc/transaction.json",
)

seastar_cc_swagger_library(
    name = "transform",
    src = "api-doc/transform.json",
)

seastar_cc_swagger_library(
    name = "usage",
    src = "api-doc/usage.json",
)
