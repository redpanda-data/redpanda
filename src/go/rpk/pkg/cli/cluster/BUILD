load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "cluster",
    srcs = [
        "cluster.go",
        "health.go",
        "info.go",
        "logdirs.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/cli/cluster/config",
        "//src/go/rpk/pkg/cli/cluster/license",
        "//src/go/rpk/pkg/cli/cluster/maintenance",
        "//src/go/rpk/pkg/cli/cluster/partitions",
        "//src/go/rpk/pkg/cli/cluster/quotas",
        "//src/go/rpk/pkg/cli/cluster/selftest",
        "//src/go/rpk/pkg/cli/cluster/storage",
        "//src/go/rpk/pkg/cli/cluster/txn",
        "//src/go/rpk/pkg/cli/group",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/kafka",
        "//src/go/rpk/pkg/out",
        "@com_github_docker_go_units//:go-units",
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_twmb_franz_go_pkg_kadm//:kadm",
        "@com_github_twmb_types//:types",
    ],
)
