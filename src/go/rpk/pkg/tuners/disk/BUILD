load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "disk",
    srcs = [
        "block_device.go",
        "block_device_darwin.go",
        "block_device_linux.go",
        "block_devices.go",
        "block_devices_darwin.go",
        "block_devices_linux.go",
        "device_features.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/disk",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/os",
        "//src/go/rpk/pkg/system",
        "//src/go/rpk/pkg/tuners/irq",
        "//src/go/rpk/pkg/utils",
        "@com_github_spf13_afero//:afero",
        "@org_uber_go_zap//:zap",
    ] + select({
        "@rules_go//go/platform:android": [
            "@org_golang_x_sys//unix",
        ],
        "@rules_go//go/platform:linux": [
            "@org_golang_x_sys//unix",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "disk_test",
    size = "small",
    srcs = [
        "block_device_test.go",
        "block_devices_test.go",
        "device_features_test.go",
    ],
    embed = [":disk"],
    deps = [
        "//src/go/rpk/pkg/os",
        "//src/go/rpk/pkg/tuners/irq",
        "//src/go/rpk/pkg/utils",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
    ],
)
