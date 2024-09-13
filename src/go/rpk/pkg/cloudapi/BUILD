load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "cloudapi",
    srcs = [
        "api_cluster.go",
        "api_installpack.go",
        "api_namespace.go",
        "api_organization.go",
        "api_virtual_cluster.go",
        "cloudapi.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloudapi",
    visibility = ["//visibility:public"],
    deps = ["//src/go/rpk/pkg/httpapi"],
)
