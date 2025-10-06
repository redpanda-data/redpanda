load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "shadow",
    srcs = [
        "config.go",
        "create.go",
        "delete.go",
        "describe.go",
        "list.go",
        "mapper.go",
        "shadow.go",
        "status.go",
        "types.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/shadow",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/os",
        "//src/go/rpk/pkg/out",
        "@build_buf_gen_go_redpandadata_core_protocolbuffers_go//redpanda/core/admin/v2:admin",
        "@build_buf_gen_go_redpandadata_core_protocolbuffers_go//redpanda/core/common",
        "@com_connectrpc_connect//:connect",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@in_gopkg_yaml_v2//:yaml_v2",
        "@org_golang_google_protobuf//types/known/durationpb",
    ],
)

go_test(
    name = "shadow_test",
    srcs = ["types_test.go"],
    embed = [":shadow"],
    deps = [
        "@build_buf_gen_go_redpandadata_core_protocolbuffers_go//redpanda/core/admin/v2:admin",
        "@com_github_stretchr_testify//require",
        "@in_gopkg_yaml_v2//:yaml_v2",
    ],
)
