load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "filesystem",
    srcs = [
        "fs.go",
        "fs_darwin.go",
        "fs_linux.go",
        "types.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/system/filesystem",
    visibility = ["//visibility:public"],
    deps = select({
        "@rules_go//go/platform:aix": [
            "@com_github_docker_go_units//:go-units",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:android": [
            "@com_github_docker_go_units//:go-units",
            "@com_github_spf13_afero//:afero",
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:darwin": [
            "@com_github_docker_go_units//:go-units",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:dragonfly": [
            "@com_github_docker_go_units//:go-units",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:freebsd": [
            "@com_github_docker_go_units//:go-units",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:illumos": [
            "@com_github_docker_go_units//:go-units",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:ios": [
            "@com_github_docker_go_units//:go-units",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:js": [
            "@com_github_docker_go_units//:go-units",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:linux": [
            "@com_github_docker_go_units//:go-units",
            "@com_github_spf13_afero//:afero",
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:netbsd": [
            "@com_github_docker_go_units//:go-units",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:openbsd": [
            "@com_github_docker_go_units//:go-units",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:plan9": [
            "@com_github_docker_go_units//:go-units",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:solaris": [
            "@com_github_docker_go_units//:go-units",
            "@com_github_spf13_afero//:afero",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "filesystem_test",
    size = "small",
    srcs = ["fs_test.go"],
    embed = [":filesystem"],
    deps = ["@com_github_spf13_afero//:afero"],
)
