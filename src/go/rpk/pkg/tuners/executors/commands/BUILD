load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "commands",
    srcs = [
        "backup_file.go",
        "command.go",
        "ethtool_change.go",
        "execute.go",
        "sysctl_set.go",
        "systemd_install_unit.go",
        "systemd_start_unit.go",
        "write_file.go",
        "write_file_lines.go",
        "write_sized_file.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/os",
        "//src/go/rpk/pkg/system/systemd",
        "//src/go/rpk/pkg/utils",
        "@com_github_lorenzosaino_go_sysctl//:go-sysctl",
        "@com_github_spf13_afero//:afero",
        "@org_uber_go_zap//:zap",
    ] + select({
        "@rules_go//go/platform:aix": [
            "//src/go/rpk/pkg/tuners/ethtool",
        ],
        "@rules_go//go/platform:android": [
            "//src/go/rpk/pkg/tuners/ethtool",
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:darwin": [
            "//src/go/rpk/pkg/tuners/ethtool",
        ],
        "@rules_go//go/platform:dragonfly": [
            "//src/go/rpk/pkg/tuners/ethtool",
        ],
        "@rules_go//go/platform:freebsd": [
            "//src/go/rpk/pkg/tuners/ethtool",
        ],
        "@rules_go//go/platform:illumos": [
            "//src/go/rpk/pkg/tuners/ethtool",
        ],
        "@rules_go//go/platform:ios": [
            "//src/go/rpk/pkg/tuners/ethtool",
        ],
        "@rules_go//go/platform:js": [
            "//src/go/rpk/pkg/tuners/ethtool",
        ],
        "@rules_go//go/platform:linux": [
            "//src/go/rpk/pkg/tuners/ethtool",
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:netbsd": [
            "//src/go/rpk/pkg/tuners/ethtool",
        ],
        "@rules_go//go/platform:openbsd": [
            "//src/go/rpk/pkg/tuners/ethtool",
        ],
        "@rules_go//go/platform:plan9": [
            "//src/go/rpk/pkg/tuners/ethtool",
        ],
        "@rules_go//go/platform:solaris": [
            "//src/go/rpk/pkg/tuners/ethtool",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "commands_test",
    size = "small",
    srcs = [
        "systemd_install_unit_test.go",
        "systemd_start_unit_test.go",
        "write_file_test.go",
        "write_sized_file_test.go",
    ],
    deps = [
        ":commands",
        "//src/go/rpk/pkg/system/systemd",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
    ],
)
