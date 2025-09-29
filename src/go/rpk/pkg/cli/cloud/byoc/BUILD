load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "byoc",
    srcs = [
        "byoc.go",
        "install.go",
        "uninstall.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud/byoc",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cobraext",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/oauth",
        "//src/go/rpk/pkg/oauth/providers/auth0",
        "//src/go/rpk/pkg/os",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/plugin",
        "//src/go/rpk/pkg/publicapi",
        "@build_buf_gen_go_redpandadata_cloud_protocolbuffers_go//redpanda/api/byocplugin/v1alpha1",
        "@com_connectrpc_connect//:connect",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "byoc_test",
    srcs = ["install_test.go"],
    embed = [":byoc"],
    deps = [
        "//src/go/rpk/pkg/config",
        "@build_buf_gen_go_redpandadata_cloud_protocolbuffers_go//redpanda/api/byocplugin/v1alpha1",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//proto",
    ],
)
