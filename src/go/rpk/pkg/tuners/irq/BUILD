load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "irq",
    srcs = [
        "balance_service.go",
        "cpu_masks.go",
        "device_info.go",
        "irq_tuner_mode.go",
        "proc_file.go",
        "utils.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/os",
        "//src/go/rpk/pkg/tuners/executors",
        "//src/go/rpk/pkg/tuners/executors/commands",
        "//src/go/rpk/pkg/tuners/hwloc",
        "//src/go/rpk/pkg/utils",
        "@com_github_spf13_afero//:afero",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "irq_test",
    size = "small",
    srcs = [
        "balance_service_test.go",
        "cpu_masks_test.go",
        "device_info_test.go",
        "proc_file_test.go",
    ],
    data = glob(["testdata/**"]),
    embed = [":irq"],
    deps = [
        "//src/go/rpk/pkg/os",
        "//src/go/rpk/pkg/tuners/executors",
        "//src/go/rpk/pkg/utils",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
    ],
)
