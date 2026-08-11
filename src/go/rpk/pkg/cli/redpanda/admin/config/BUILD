load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "config",
    srcs = ["config.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/redpanda/admin/config",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/redpanda",
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@org_golang_x_sync//errgroup",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "config_test",
    srcs = ["config_test.go"],
    embed = [":config"],
    deps = [
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
    ],
)
