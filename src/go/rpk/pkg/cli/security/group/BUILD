load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "group",
    srcs = [
        "assign.go",
        "describe.go",
        "group.go",
        "list.go",
        "unassign.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/security/group",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/redpanda",
        "@build_buf_gen_go_redpandadata_core_protocolbuffers_go//redpanda/core/admin/v2:admin",
        "@com_connectrpc_connect//:connect",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
    ],
)

go_test(
    name = "group_test",
    srcs = ["group_test.go"],
    embed = [":group"],
    deps = [
        "@build_buf_gen_go_redpandadata_core_protocolbuffers_go//redpanda/core/admin/v2:admin",
        "@com_github_stretchr_testify//require",
    ],
)
