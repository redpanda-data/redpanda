load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "connections",
    srcs = [
        "connections.go",
        "list.go",
        "types.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster/connections",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/publicapi",
        "//src/go/rpk/pkg/redpanda",
        "@build_buf_gen_go_redpandadata_core_protocolbuffers_go//redpanda/core/admin/v2:admin",
        "@com_connectrpc_connect//:connect",
        "@com_github_docker_go_units//:go-units",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
    ],
)

go_test(
    name = "connections_test",
    srcs = [
        "connections_test.go",
        "list_test.go",
    ],
    embed = [":connections"],
    deps = [
        "@build_buf_gen_go_redpandadata_core_protocolbuffers_go//redpanda/core/admin/v2:admin",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
