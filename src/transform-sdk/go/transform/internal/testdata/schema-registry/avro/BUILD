load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "avro",
    srcs = [
        "example.go",
        "generate.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/internal/testdata/schema-registry/avro",
    visibility = ["//src/transform-sdk/go/transform:__subpackages__"],
    deps = [
        "@com_github_actgardner_gogen_avro_v10//compiler",
        "@com_github_actgardner_gogen_avro_v10//vm",
        "@com_github_actgardner_gogen_avro_v10//vm/types",
    ],
)
