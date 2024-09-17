load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "cli",
    srcs = [
        "deprecated.go",
        "plugin_cmds.go",
        "root.go",
        "root_darwin.go",
        "root_linux.go",
        "root_windows.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cli/acl",
        "//src/go/rpk/pkg/cli/cloud",
        "//src/go/rpk/pkg/cli/cluster",
        "//src/go/rpk/pkg/cli/connect",
        "//src/go/rpk/pkg/cli/container",
        "//src/go/rpk/pkg/cli/debug",
        "//src/go/rpk/pkg/cli/generate",
        "//src/go/rpk/pkg/cli/group",
        "//src/go/rpk/pkg/cli/plugin",
        "//src/go/rpk/pkg/cli/profile",
        "//src/go/rpk/pkg/cli/registry",
        "//src/go/rpk/pkg/cli/security",
        "//src/go/rpk/pkg/cli/topic",
        "//src/go/rpk/pkg/cli/transform",
        "//src/go/rpk/pkg/cli/version",
        "//src/go/rpk/pkg/cobraext",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/plugin",
        "@com_github_fatih_color//:color",
        "@com_github_moby_term//:term",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@org_golang_x_term//:term",
        "@org_uber_go_zap//:zap",
    ] + select({
        "@rules_go//go/platform:android": [
            "//src/go/rpk/pkg/cli/iotune",
            "//src/go/rpk/pkg/cli/redpanda",
            "//src/go/rpk/pkg/cli/redpanda/tune",
            "//src/go/rpk/pkg/redpanda",
        ],
        "@rules_go//go/platform:darwin": [
            "//src/go/rpk/pkg/cli/redpanda",
        ],
        "@rules_go//go/platform:ios": [
            "//src/go/rpk/pkg/cli/redpanda",
        ],
        "@rules_go//go/platform:linux": [
            "//src/go/rpk/pkg/cli/iotune",
            "//src/go/rpk/pkg/cli/redpanda",
            "//src/go/rpk/pkg/cli/redpanda/tune",
            "//src/go/rpk/pkg/redpanda",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "cli_test",
    size = "small",
    srcs = ["root_test.go"],
    embed = [":cli"],
    deps = [
        "@com_github_spf13_cobra//:cobra",
        "@com_github_stretchr_testify//assert",
    ],
)
