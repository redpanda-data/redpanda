load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "factory",
    srcs = ["factory.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/factory",
    visibility = ["//visibility:public"],
    deps = select({
        "@rules_go//go/platform:android": [
            "//src/go/rpk/pkg/cloud/gcp",
            "//src/go/rpk/pkg/config",
            "//src/go/rpk/pkg/net",
            "//src/go/rpk/pkg/os",
            "//src/go/rpk/pkg/system",
            "//src/go/rpk/pkg/tuners",
            "//src/go/rpk/pkg/tuners/ballast",
            "//src/go/rpk/pkg/tuners/coredump",
            "//src/go/rpk/pkg/tuners/cpu",
            "//src/go/rpk/pkg/tuners/disk",
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/tuners/executors",
            "//src/go/rpk/pkg/tuners/hwloc",
            "//src/go/rpk/pkg/tuners/irq",
            "@com_github_spf13_afero//:afero",
            "@org_uber_go_zap//:zap",
        ],
        "@rules_go//go/platform:linux": [
            "//src/go/rpk/pkg/cloud/gcp",
            "//src/go/rpk/pkg/config",
            "//src/go/rpk/pkg/net",
            "//src/go/rpk/pkg/os",
            "//src/go/rpk/pkg/system",
            "//src/go/rpk/pkg/tuners",
            "//src/go/rpk/pkg/tuners/ballast",
            "//src/go/rpk/pkg/tuners/coredump",
            "//src/go/rpk/pkg/tuners/cpu",
            "//src/go/rpk/pkg/tuners/disk",
            "//src/go/rpk/pkg/tuners/ethtool",
            "//src/go/rpk/pkg/tuners/executors",
            "//src/go/rpk/pkg/tuners/hwloc",
            "//src/go/rpk/pkg/tuners/irq",
            "@com_github_spf13_afero//:afero",
            "@org_uber_go_zap//:zap",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "factory_test",
    size = "small",
    srcs = ["factory_test.go"],
    deps = select({
        "@rules_go//go/platform:android": [
            ":factory",
            "//src/go/rpk/pkg/config",
            "@com_github_stretchr_testify//require",
        ],
        "@rules_go//go/platform:linux": [
            ":factory",
            "//src/go/rpk/pkg/config",
            "@com_github_stretchr_testify//require",
        ],
        "//conditions:default": [],
    }),
)
