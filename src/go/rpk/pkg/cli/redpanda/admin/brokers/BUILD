load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "brokers",
    srcs = [
        "brokers.go",
        "decommission.go",
        "decommission-status.go",
        "list.go",
        "recommission.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/redpanda/admin/brokers",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/redpanda",
        "@com_github_docker_go_units//:go-units",
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_twmb_types//:types",
    ],
)
