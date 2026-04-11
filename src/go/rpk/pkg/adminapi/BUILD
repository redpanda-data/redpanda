load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "adminapi",
    srcs = [
        "admin.go",
        "version.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/oauth/authtoken",
        "//src/go/rpk/pkg/oauth/providers/auth0",
        "//src/go/rpk/pkg/redpanda",
        "@build_buf_gen_go_redpandadata_core_protocolbuffers_go//redpanda/core/admin/v2:admin",
        "@com_connectrpc_connect//:connect",
        "@com_github_kr_text//:text",
        "@com_github_moby_term//:term",
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_spf13_afero//:afero",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "adminapi_test",
    size = "small",
    srcs = ["admin_test.go"],
    embed = [":adminapi"],
    env = {"HOME": "/"},
    deps = [
        "//src/go/rpk/pkg/config",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
    ],
)
