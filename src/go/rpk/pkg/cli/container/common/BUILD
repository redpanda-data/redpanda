load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "common",
    srcs = [
        "client.go",
        "common.go",
        "context.go",
        "test.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/common",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/config",
        "@com_github_docker_docker//api/types",
        "@com_github_docker_docker//api/types/container",
        "@com_github_docker_docker//api/types/filters",
        "@com_github_docker_docker//api/types/image",
        "@com_github_docker_docker//api/types/network",
        "@com_github_docker_docker//client",
        "@com_github_docker_go_connections//nat",
        "@com_github_opencontainers_go_digest//:go-digest",
        "@com_github_opencontainers_image_spec//specs-go/v1:specs-go",
        "@com_github_spf13_afero//:afero",
        "@in_gopkg_yaml_v3//:yaml_v3",
        "@org_uber_go_zap//:zap",
    ],
)
