load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "selftest",
    srcs = [
        "selftest.go",
        "start.go",
        "status.go",
        "stop.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster/selftest",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/system",
        "@com_github_docker_go_units//:go-units",
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
    ],
)

go_test(
    name = "selftest_test",
    size = "small",
    srcs = ["selftest_test.go"],
    embed = [":selftest"],
    deps = [
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_stretchr_testify//require",
    ],
)
