load("//bazel:build.bzl", "redpanda_cc_library")

package(default_visibility = [
    "//src/v/redpanda:__subpackages__",
])

redpanda_cc_library(
    name = "debug",
    srcs = ["debug.cc"],
    hdrs = ["debug.h"],
    deps = [
        "//proto/redpanda/core/admin/internal/v1:debug_redpanda_proto",
        "//src/v/base",
        "//src/v/finjector",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "//src/v/utils:to_string",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "breakglass",
    srcs = ["breakglass.cc"],
    hdrs = ["breakglass.h"],
    deps = [
        "//proto/redpanda/core/admin/internal/v1:breakglass_redpanda_proto",
        "//src/v/base",
        "//src/v/cluster",
        "//src/v/finjector",
        "//src/v/model",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "//src/v/utils:to_string",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "metastore",
    srcs = ["metastore.cc"],
    hdrs = ["metastore.h"],
    deps = [
        "//proto/redpanda/core/admin/internal/cloud_topics/v1:metastore_redpanda_proto",
        "//src/v/base",
        "//src/v/cloud_topics/level_one/metastore:replicated_metastore",
        "//src/v/cluster",
        "//src/v/serde/protobuf:rpc",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "level_zero_gc",
    srcs = ["level_zero_gc.cc"],
    hdrs = ["level_zero_gc.h"],
    deps = [
        "//proto/redpanda/core/admin/internal/cloud_topics/v1:level_zero_gc_redpanda_proto",
        "//src/v/base",
        "//src/v/cloud_topics:state_accessors",
        "//src/v/cloud_topics/frontend",
        "//src/v/cloud_topics/level_zero/gc:level_zero_gc",
        "//src/v/cluster",
        "//src/v/model",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/redpanda/admin/proxy:context",
        "//src/v/serde/protobuf:rpc",
        "//src/v/ssx:sformat",
        "@fmt",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "shadow_link_internal",
    srcs = ["shadow_link_internal.cc"],
    hdrs = ["shadow_link_internal.h"],
    implementation_deps = [
        "//src/v/cluster_link",
        "//src/v/redpanda/admin/services:utils",
        "//src/v/redpanda/admin/services/shadow_link:err",
    ],
    deps = [
        "//proto/redpanda/core/admin/internal/shadow_link_internal/v1:shadow_link_internal_redpanda_proto",
        "//src/v/cluster",
        "//src/v/cluster_link:fwd",
        "//src/v/cluster_link/model",
        "//src/v/model",
        "//src/v/redpanda/admin/proxy:client",
        "//src/v/serde/protobuf:rpc",
        "@seastar",
    ],
)
