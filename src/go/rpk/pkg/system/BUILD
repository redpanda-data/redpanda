load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "system",
    srcs = [
        "cgroup.go",
        "cpu.go",
        "grub.go",
        "memory.go",
        "memory_darwin.go",
        "memory_linux.go",
        "memory_windows.go",
        "metrics.go",
        "metrics_windows.go",
        "ntp.go",
        "runtime_options.go",
        "utils.go",
        "utils_darwin.go",
        "utils_linux.go",
        "utils_windows.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/system",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/os",
        "//src/go/rpk/pkg/tuners/executors",
        "//src/go/rpk/pkg/tuners/executors/commands",
        "//src/go/rpk/pkg/utils",
        "@com_github_docker_go_units//:go-units",
        "@com_github_spf13_afero//:afero",
        "@org_uber_go_zap//:zap",
    ] + select({
        "@rules_go//go/platform:aix": [
            "//src/go/rpk/pkg/config",
            "@com_github_hashicorp_go_multierror//:go-multierror",
            "@com_github_tklauser_go_sysconf//:go-sysconf",
        ],
        "@rules_go//go/platform:android": [
            "//src/go/rpk/pkg/config",
            "@com_github_hashicorp_go_multierror//:go-multierror",
            "@com_github_tklauser_go_sysconf//:go-sysconf",
        ],
        "@rules_go//go/platform:darwin": [
            "//src/go/rpk/pkg/config",
            "@com_github_hashicorp_go_multierror//:go-multierror",
            "@com_github_tklauser_go_sysconf//:go-sysconf",
        ],
        "@rules_go//go/platform:dragonfly": [
            "//src/go/rpk/pkg/config",
            "@com_github_hashicorp_go_multierror//:go-multierror",
            "@com_github_tklauser_go_sysconf//:go-sysconf",
        ],
        "@rules_go//go/platform:freebsd": [
            "//src/go/rpk/pkg/config",
            "@com_github_hashicorp_go_multierror//:go-multierror",
            "@com_github_tklauser_go_sysconf//:go-sysconf",
        ],
        "@rules_go//go/platform:illumos": [
            "//src/go/rpk/pkg/config",
            "@com_github_hashicorp_go_multierror//:go-multierror",
            "@com_github_tklauser_go_sysconf//:go-sysconf",
        ],
        "@rules_go//go/platform:ios": [
            "//src/go/rpk/pkg/config",
            "@com_github_hashicorp_go_multierror//:go-multierror",
            "@com_github_tklauser_go_sysconf//:go-sysconf",
        ],
        "@rules_go//go/platform:js": [
            "//src/go/rpk/pkg/config",
            "@com_github_hashicorp_go_multierror//:go-multierror",
            "@com_github_tklauser_go_sysconf//:go-sysconf",
        ],
        "@rules_go//go/platform:linux": [
            "//src/go/rpk/pkg/config",
            "@com_github_hashicorp_go_multierror//:go-multierror",
            "@com_github_tklauser_go_sysconf//:go-sysconf",
        ],
        "@rules_go//go/platform:netbsd": [
            "//src/go/rpk/pkg/config",
            "@com_github_hashicorp_go_multierror//:go-multierror",
            "@com_github_tklauser_go_sysconf//:go-sysconf",
        ],
        "@rules_go//go/platform:openbsd": [
            "//src/go/rpk/pkg/config",
            "@com_github_hashicorp_go_multierror//:go-multierror",
            "@com_github_tklauser_go_sysconf//:go-sysconf",
        ],
        "@rules_go//go/platform:plan9": [
            "//src/go/rpk/pkg/config",
            "@com_github_hashicorp_go_multierror//:go-multierror",
            "@com_github_tklauser_go_sysconf//:go-sysconf",
        ],
        "@rules_go//go/platform:solaris": [
            "//src/go/rpk/pkg/config",
            "@com_github_hashicorp_go_multierror//:go-multierror",
            "@com_github_tklauser_go_sysconf//:go-sysconf",
        ],
        "@rules_go//go/platform:windows": [
            "//src/go/rpk/pkg/config",
        ],
        "//conditions:default": [],
    }),
)

go_test(
    name = "system_test",
    size = "small",
    srcs = [
        "cgroup_test.go",
        "cpu_test.go",
        "grub_test.go",
        "metrics_test.go",
        "ntp_test.go",
        "runtime_options_test.go",
    ],
    data = glob(["testdata/**"]),
    embed = [":system"],
    deps = [
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/tuners/executors",
        "//src/go/rpk/pkg/utils",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
