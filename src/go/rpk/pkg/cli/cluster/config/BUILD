load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "config",
    srcs = [
        "config.go",
        "edit.go",
        "export.go",
        "get.go",
        "import.go",
        "lint.go",
        "reset.go",
        "set.go",
        "status.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster/config",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "@com_github_kballard_go_shellquote//:go-shellquote",
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@in_gopkg_yaml_v3//:yaml_v3",
    ],
)

go_test(
    name = "config_test",
    size = "small",
    srcs = [
        "edit_test.go",
        "import_test.go",
    ],
    embed = [":config"],
    deps = [
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@in_gopkg_yaml_v3//:yaml_v3",
    ],
)
