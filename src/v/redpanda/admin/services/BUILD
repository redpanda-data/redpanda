load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = ["//src/v/redpanda:__subpackages__"])

redpanda_cc_library(
    name = "utils",
    srcs = ["utils.cc"],
    hdrs = ["utils.h"],
    deps = [
        "//src/v/cluster",
        "//src/v/features",
        "//src/v/model",
        "//src/v/serde/protobuf:rpc",
    ],
)

redpanda_cc_library(
    name = "broker",
    srcs = ["broker.cc"],
    hdrs = ["broker.h"],
    deps = [
        ":utils",
        "//proto/redpanda/core/admin/v2:broker_redpanda_proto",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "//src/v/version",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "cluster",
    srcs = ["cluster.cc"],
    hdrs = ["cluster.h"],
    implementation_deps = [
        "//src/v/redpanda/admin/proxy:context",
    ],
    deps = [
        ":utils",
        "//proto/redpanda/core/admin/v2:cluster_redpanda_proto",
        "//src/v/features",
        "//src/v/redpanda/admin:kafka_connections_service",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/resource_mgmt:available_memory",
        "//src/v/serde/protobuf:rpc",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "security",
    srcs = ["security.cc"],
    hdrs = ["security.h"],
    implementation_deps = [
        ":utils",
        "//src/v/redpanda/admin/proxy:context",
        "//src/v/security:request_auth",
    ],
    deps = [
        "//proto/redpanda/core/admin/v2:security_redpanda_proto",
        "//src/v/cluster",
        "//src/v/kafka/server",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/security",
        "//src/v/serde/protobuf:rpc",
        "@seastar",
    ],
)
