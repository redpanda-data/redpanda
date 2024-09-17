load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "plugin",
    srcs = [
        "managed.go",
        "manifest.go",
        "plugin.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/httpapi",
        "//src/go/rpk/pkg/os",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@in_gopkg_yaml_v3//:yaml_v3",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "plugin_test",
    size = "small",
    srcs = [
        "manifest_test.go",
        "plugin_test.go",
    ],
    embed = [":plugin"],
    deps = [
        "//src/go/rpk/pkg/testfs",
        "@com_github_stretchr_testify//require",
    ],
)
