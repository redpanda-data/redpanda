load("//bazel:build.bzl", "redpanda_cc_library")
load("//src/v/rpc:compiler.bzl", "redpanda_cc_rpc_library")

redpanda_cc_rpc_library(
    name = "raft_rpc",
    src = "raftgen.json",
)

redpanda_cc_library(
    name = "fundamental",
    hdrs = [
        "fundamental.h",
    ],
    include_prefix = "raft",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/utils:named_type",
    ],
)
