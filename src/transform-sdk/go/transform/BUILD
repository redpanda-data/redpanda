load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "transform",
    srcs = [
        "abi.go",  # keep: gazelle tries to remove this
        "doc.go",
        "processor.go",
        "sdk.go",
        "serialize.go",
        "stub_abi.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/transform-sdk/go/transform",
    visibility = ["//visibility:public"],
    deps = ["//src/transform-sdk/go/transform/internal/rwbuf"],
)

go_test(
    name = "transform_test",
    size = "small",
    srcs = [
        "example_mirror_test.go",
        "example_regexp_filter_test.go",
        "example_transcoding_test.go",
        "example_validation_test.go",
        "serialize_test.go",
    ],
    embed = [":transform"],
    deps = ["//src/transform-sdk/go/transform/internal/rwbuf"],
)
