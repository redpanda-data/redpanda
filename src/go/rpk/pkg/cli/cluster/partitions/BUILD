load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "partitions",
    srcs = [
        "balancer_run.go",
        "cancel.go",
        "cancel_hidden.go",
        "list.go",
        "move.go",
        "move_status.go",
        "partitions.go",
        "status.go",
        "toggle.go",
        "transfer_leadership.go",
        "unsafe_recover.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster/partitions",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "@com_github_docker_go_units//:go-units",
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_twmb_types//:types",
        "@org_golang_x_sync//errgroup",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "partitions_test",
    size = "small",
    srcs = ["move_test.go"],
    embed = [":partitions"],
    deps = [
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_stretchr_testify//require",
    ],
)
