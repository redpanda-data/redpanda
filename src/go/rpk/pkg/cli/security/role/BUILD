load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "role",
    srcs = [
        "assign.go",
        "create.go",
        "delete.go",
        "describe.go",
        "list.go",
        "role.go",
        "unassign.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/security/role",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/kafka",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/publicapi",
        "//src/go/rpk/pkg/redpanda",
        "@build_buf_gen_go_redpandadata_core_protocolbuffers_go//redpanda/core/admin/v2:admin",
        "@build_buf_gen_go_redpandadata_dataplane_protocolbuffers_go//redpanda/api/dataplane/v1:dataplane",
        "@com_connectrpc_connect//:connect",
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_twmb_franz_go_pkg_kadm//:kadm",
        "@com_github_twmb_types//:types",
    ],
)
