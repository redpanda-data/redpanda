load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "resourcegroup",
    srcs = [
        "create.go",
        "delete.go",
        "list.go",
        "resourcegroup.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud/resourcegroup",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/oauth",
        "//src/go/rpk/pkg/oauth/providers/auth0",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/publicapi",
        "@build_buf_gen_go_redpandadata_cloud_protocolbuffers_go//redpanda/api/controlplane/v1beta2",
        "@com_connectrpc_connect//:connect",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
    ],
)
