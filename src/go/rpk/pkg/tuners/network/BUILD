load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "network",
    srcs = [
        "const.go",
        "nic.go",
        "nics.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/network",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/tuners/irq",
        "@org_uber_go_zap//:zap",
    ] + select({
        "@rules_go//go/platform:aix": [
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/utils",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:android": [
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/utils",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:darwin": [
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/utils",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:dragonfly": [
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/utils",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:freebsd": [
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/utils",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:illumos": [
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/utils",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:ios": [
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/utils",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:js": [
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/utils",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:linux": [
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/utils",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:netbsd": [
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/utils",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:openbsd": [
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/utils",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:plan9": [
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/utils",
            "@com_github_spf13_afero//:afero",
        ],
        "@rules_go//go/platform:solaris": [
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/utils",
            "@com_github_spf13_afero//:afero",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "network_test",
    size = "small",
    srcs = ["nic_test.go"],
    embed = [":network"],
    deps = [
        "//src/go/rpk/pkg/tuners/ethtool",
        "//src/go/rpk/pkg/tuners/irq",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
    ],
)
