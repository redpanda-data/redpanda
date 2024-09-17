load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "os",
    srcs = [
        "commands.go",
        "directory.go",
        "file.go",
        "file_all.go",
        "file_windows.go",
        "lock.go",
        "proc.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/utils",
        "@com_github_spf13_afero//:afero",
        "@in_gopkg_yaml_v3//:yaml_v3",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "os_test",
    size = "small",
    srcs = ["proc_test.go"],
    deps = [
        ":os",
        "//src/go/rpk/pkg/utils",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
    ],
)
