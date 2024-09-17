load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "redpanda",
    srcs = [
        "launcher.go",
        "paths.go",
        "version.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_spf13_afero//:afero",
        "@org_uber_go_zap//:zap",
    ] + select({
        "@rules_go//go/platform:aix": [
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:android": [
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:darwin": [
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:dragonfly": [
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:freebsd": [
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:illumos": [
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:ios": [
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:js": [
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:linux": [
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:netbsd": [
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:openbsd": [
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:plan9": [
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:solaris": [
            "@org_golang_x_sys//unix",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "redpanda_test",
    size = "small",
    srcs = [
        "launcher_test.go",
        "version_test.go",
    ],
    embed = [":redpanda"],
    deps = ["@com_github_stretchr_testify//require"],
)
