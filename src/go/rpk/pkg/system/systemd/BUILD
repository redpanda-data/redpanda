load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "systemd",
    srcs = [
        "client.go",
        "dbus.go",
        "mock.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/system/systemd",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/utils",
        "@com_github_coreos_go_systemd_v22//dbus",
        "@com_github_spf13_afero//:afero",
    ],
)
