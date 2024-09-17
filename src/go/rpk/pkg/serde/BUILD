load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "serde",
    srcs = [
        "avro.go",
        "jsonschema.go",
        "proto.go",
        "serde.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/serde",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_bufbuild_protocompile//:protocompile",
        "@com_github_bufbuild_protocompile//linker",
        "@com_github_hamba_avro_v2//:avro",
        "@com_github_linkedin_goavro_v2//:goavro",
        "@com_github_santhosh_tekuri_jsonschema_v6//:jsonschema",
        "@com_github_twmb_franz_go_pkg_sr//:sr",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//reflect/protoreflect",
        "@org_golang_google_protobuf//types/dynamicpb",
    ],
)

go_test(
    name = "serde_test",
    size = "small",
    srcs = [
        "avro_test.go",
        "jsonschema_test.go",
        "proto_test.go",
    ],
    embed = [":serde"],
    deps = [
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/schemaregistry",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
        "@com_github_twmb_franz_go_pkg_sr//:sr",
    ],
)
