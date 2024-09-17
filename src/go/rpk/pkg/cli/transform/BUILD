load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "transform",
    srcs = [
        "build.go",
        "delete.go",
        "deploy.go",
        "init.go",
        "list.go",
        "logs.go",
        "meta.go",
        "transform.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/cli/transform/buildpack",
        "//src/go/rpk/pkg/cli/transform/project",
        "//src/go/rpk/pkg/cli/transform/template",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/httpapi",
        "//src/go/rpk/pkg/kafka",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/publicapi",
        "@build_buf_gen_go_redpandadata_dataplane_protocolbuffers_go//redpanda/api/dataplane/v1alpha1",
        "@com_connectrpc_connect//:connect",
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_twmb_franz_go//pkg/kgo",
        "@com_github_twmb_franz_go_pkg_kadm//:kadm",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "transform_test",
    size = "small",
    srcs = [
        "list_test.go",
        "logs_test.go",
    ],
    embed = [":transform"],
    deps = [
        "//src/go/rpk/pkg/config",
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@com_github_twmb_franz_go//pkg/kgo",
        "@in_gopkg_yaml_v3//:yaml_v3",
    ],
)
