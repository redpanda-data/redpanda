load("//bazel:build.bzl", "redpanda_cc_library")
load("//src/v/rpc:compiler.bzl", "redpanda_cc_rpc_library")

redpanda_cc_library(
    name = "types",
    hdrs = [
        "types.h",
    ],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:enum",
        "//src/v/serde:iobuf",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "context",
    srcs = ["context.cc"],
    hdrs = ["context.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/model",
        "//src/v/serde/protobuf:rpc",
    ],
)

redpanda_cc_rpc_library(
    name = "proxy_rpc",
    src = "proxy_service.json",
    out = "proxy_service.h",
    deps = [
        ":types",
    ],
)

redpanda_cc_library(
    name = "client",
    srcs = [
        "client.cc",
    ],
    hdrs = [
        "client.h",
    ],
    implementation_deps = [
        ":context",
        ":proxy_rpc",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/model",
        "//src/v/rpc",
        "//src/v/serde",
        "//src/v/serde/protobuf:rpc",
        "@abseil-cpp//absl/container:flat_hash_map",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "service",
    srcs = [
        "service.cc",
    ],
    hdrs = [
        "service.h",
    ],
    implementation_deps = [
        ":context",
        "//src/v/rpc",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":proxy_rpc",
        ":types",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde/protobuf:rpc",
        "@seastar",
    ],
)
