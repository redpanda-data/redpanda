load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "ai",
    srcs = [
        "ai.go",
        "client.go",
        "hook.go",
        "install.go",
        "uninstall.go",
        "upgrade.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/ai",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cobraext",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/fips",
        "//src/go/rpk/pkg/httpapi",
        "//src/go/rpk/pkg/oauth",
        "//src/go/rpk/pkg/oauth/providers/auth0",
        "//src/go/rpk/pkg/osutil",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/plugin",
        "//src/go/rpk/pkg/publicapi",
        "//src/go/rpk/pkg/redpanda",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "ai_test",
    srcs = [
        "client_test.go",
        "hook_test.go",
        "install_test.go",
    ],
    embed = [":ai"],
    deps = [
        "//src/go/rpk/pkg/config",
        "@build_buf_gen_go_redpandadata_cloud_protocolbuffers_go//redpanda/api/controlplane/v1",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//proto",
    ],
)
