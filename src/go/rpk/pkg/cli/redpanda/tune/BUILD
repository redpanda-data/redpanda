load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "tune",
    srcs = [
        "help.go",
        "list.go",
        "tune.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/redpanda/tune",
    visibility = ["//visibility:public"],
    deps = select({
        "@rules_go//go/platform:android": [
            "//src/go/rpk/pkg/config",
            "//src/go/rpk/pkg/out",
            "//src/go/rpk/pkg/tuners/factory",
            "//src/go/rpk/pkg/tuners/hwloc",
            "//src/go/rpk/pkg/tuners/irq",
            "//src/go/rpk/pkg/utils",
            "@com_github_fatih_color//:color",
            "@com_github_spf13_afero//:afero",
            "@com_github_spf13_cobra//:cobra",
            "@org_uber_go_zap//:zap",
        ],
        "@rules_go//go/platform:linux": [
            "//src/go/rpk/pkg/config",
            "//src/go/rpk/pkg/out",
            "//src/go/rpk/pkg/tuners/factory",
            "//src/go/rpk/pkg/tuners/hwloc",
            "//src/go/rpk/pkg/tuners/irq",
            "//src/go/rpk/pkg/utils",
            "@com_github_fatih_color//:color",
            "@com_github_spf13_afero//:afero",
            "@com_github_spf13_cobra//:cobra",
            "@org_uber_go_zap//:zap",
        ],
        "//conditions:default": [],
    }),
)
