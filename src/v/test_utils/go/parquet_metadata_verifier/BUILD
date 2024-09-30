load("@rules_go//go:def.bzl", "go_binary", "go_library")

go_binary(
    name = "parquet_metadata_verifier",
    embed = [":parquet_metadata_verifier_lib"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "parquet_metadata_verifier_lib",
    srcs = ["main.go"],
    importpath = "github.com/redpanda-data/redpanda/src/v/test_utils/go/parquet_metadata_verifier",
    visibility = ["//visibility:private"],
    deps = [
        "@com_github_kr_pretty//:pretty",
        "@com_github_parquet_go_parquet_go//deprecated",
        "@com_github_parquet_go_parquet_go//format",
        "@com_github_segmentio_encoding//thrift",
    ],
)
