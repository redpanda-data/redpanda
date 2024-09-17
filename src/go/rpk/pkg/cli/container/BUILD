load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "container",
    srcs = [
        "container.go",
        "purge.go",
        "start.go",
        "status.go",
        "stop.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cli/container/common",
        "//src/go/rpk/pkg/cli/profile",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/httpapi",
        "//src/go/rpk/pkg/net",
        "//src/go/rpk/pkg/out",
        "@com_github_avast_retry_go//:retry-go",
        "@com_github_docker_docker//api/types/container",
        "@com_github_docker_docker//pkg/stdcopy",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_twmb_franz_go//pkg/kgo",
        "@com_github_twmb_franz_go_pkg_kadm//:kadm",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "container_test",
    size = "small",
    srcs = ["start_test.go"],
    embed = [":container"],
    deps = ["@com_github_stretchr_testify//require"],
)
