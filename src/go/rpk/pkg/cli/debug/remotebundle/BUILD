load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "remotebundle",
    srcs = [
        "cancel.go",
        "download.go",
        "remote.go",
        "start.go",
        "status.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/remotebundle",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/cli/debug/common",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "@com_github_docker_go_units//:go-units",
        "@com_github_google_uuid//:uuid",
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@io_k8s_apimachinery//pkg/labels",
        "@org_golang_x_sync//errgroup",
    ],
)
