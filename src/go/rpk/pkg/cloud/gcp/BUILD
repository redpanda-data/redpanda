load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "gcp",
    srcs = ["gcp.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/gcp",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cloud/provider",
        "@com_google_cloud_go_compute_metadata//:metadata",
    ],
)
