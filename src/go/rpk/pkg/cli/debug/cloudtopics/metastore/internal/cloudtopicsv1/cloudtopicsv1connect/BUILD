load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "cloudtopicsv1connect",
    srcs = ["metastore.connect.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/cloudtopics/metastore/internal/cloudtopicsv1/cloudtopicsv1connect",
    visibility = ["//src/go/rpk/pkg/cli/debug/cloudtopics/metastore:__subpackages__"],
    deps = [
        "//src/go/rpk/pkg/cli/debug/cloudtopics/metastore/internal/cloudtopicsv1",
        "@com_connectrpc_connect//:connect",
    ],
)
