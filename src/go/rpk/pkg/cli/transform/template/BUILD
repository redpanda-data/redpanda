load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "template",
    srcs = [
        "golang_template.go",
        "js_template.go",
        "rust_template.go",
    ],
    embedsrcs = [
        "golang/README.md",
        "golang/transform.gosrc",
        "js/README.md",
        "js/esbuild.js",
        "js/transform.jssrc",
        "js/transform.tssrc",
        "js/tsconfig.json",
        "rust/README.md",
        "rust/main.rustsrc",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform/template",
    visibility = ["//visibility:public"],
)
