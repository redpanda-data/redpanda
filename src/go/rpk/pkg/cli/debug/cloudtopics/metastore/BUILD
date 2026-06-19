load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "metastore",
    srcs = [
        "files.go",
        "flags.go",
        "layout.go",
        "metastore.go",
        "transport.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/cloudtopics/metastore",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/cli/debug/cloudtopics/metastore/internal/cloudtopicsv1",
        "//src/go/rpk/pkg/cli/debug/cloudtopics/metastore/internal/cloudtopicsv1/cloudtopicsv1connect",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/out",
        "@com_connectrpc_connect//:connect",
        "@com_github_redpanda_data_common_go_rpadmin//:rpadmin",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
    ],
)

go_test(
    name = "metastore_test",
    srcs = [
        "files_test.go",
        "flags_test.go",
        "layout_test.go",
    ],
    embed = [":metastore"],
    deps = [
        "//src/go/rpk/pkg/cli/debug/cloudtopics/metastore/internal/cloudtopicsv1",
        "@com_github_stretchr_testify//require",
    ],
)
