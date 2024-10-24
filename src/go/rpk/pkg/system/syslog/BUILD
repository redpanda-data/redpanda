load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "syslog",
    srcs = [
        "package.go",
        "syslog.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/system/syslog",
    visibility = ["//visibility:public"],
    deps = select({
        "@rules_go//go/platform:android": [
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:linux": [
            "@org_golang_x_sys//unix",
        ],
        "//conditions:default": [],
    }),
)
