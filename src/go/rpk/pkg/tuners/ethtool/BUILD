load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "ethtool",
    srcs = ["ethtool.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool",
    visibility = ["//visibility:public"],
    deps = select({
        "@rules_go//go/platform:aix": [
            "@com_github_safchain_ethtool//:ethtool",
        ],
        "@rules_go//go/platform:android": [
            "@com_github_safchain_ethtool//:ethtool",
        ],
        "@rules_go//go/platform:darwin": [
            "@com_github_safchain_ethtool//:ethtool",
        ],
        "@rules_go//go/platform:dragonfly": [
            "@com_github_safchain_ethtool//:ethtool",
        ],
        "@rules_go//go/platform:freebsd": [
            "@com_github_safchain_ethtool//:ethtool",
        ],
        "@rules_go//go/platform:illumos": [
            "@com_github_safchain_ethtool//:ethtool",
        ],
        "@rules_go//go/platform:ios": [
            "@com_github_safchain_ethtool//:ethtool",
        ],
        "@rules_go//go/platform:js": [
            "@com_github_safchain_ethtool//:ethtool",
        ],
        "@rules_go//go/platform:linux": [
            "@com_github_safchain_ethtool//:ethtool",
        ],
        "@rules_go//go/platform:netbsd": [
            "@com_github_safchain_ethtool//:ethtool",
        ],
        "@rules_go//go/platform:openbsd": [
            "@com_github_safchain_ethtool//:ethtool",
        ],
        "@rules_go//go/platform:plan9": [
            "@com_github_safchain_ethtool//:ethtool",
        ],
        "@rules_go//go/platform:solaris": [
            "@com_github_safchain_ethtool//:ethtool",
        ],
        "//conditions:default": [],
    }),
)
