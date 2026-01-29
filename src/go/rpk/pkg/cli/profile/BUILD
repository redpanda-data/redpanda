load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "profile",
    srcs = [
        "clear.go",
        "create.go",
        "current.go",
        "delete.go",
        "edit.go",
        "edit_gobals.go",
        "list.go",
        "print.go",
        "print_globals.go",
        "profile.go",
        "prompt.go",
        "rename.go",
        "set.go",
        "set_globals.go",
        "use.go",
        "validate.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/profile",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/cli/container/common",
        "//src/go/rpk/pkg/cobraext",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/oauth/authtoken",
        "//src/go/rpk/pkg/oauth/providers/auth0",
        "//src/go/rpk/pkg/os",
        "//src/go/rpk/pkg/out",
        "//src/go/rpk/pkg/publicapi",
        "@build_buf_gen_go_redpandadata_cloud_protocolbuffers_go//redpanda/api/controlplane/v1:controlplane",
        "@com_github_fatih_color//:color",
        "@com_github_rs_xid//:xid",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@in_gopkg_yaml_v3//:yaml_v3",
    ],
)

go_test(
    name = "profile_test",
    size = "small",
    srcs = [
        "create_test.go",
        "print_test.go",
        "prompt_test.go",
        "validate_test.go",
    ],
    embed = [":profile"],
    env = {
        # We use a fake fs in tests, but we still need to configure a valid config path
        "XDG_CONFIG_HOME": "/.test-config",
    },
    deps = [
        "//src/go/rpk/pkg/config",
        "@build_buf_gen_go_redpandadata_cloud_protocolbuffers_go//redpanda/api/controlplane/v1:controlplane",
        "@com_github_fatih_color//:color",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
    ],
)
