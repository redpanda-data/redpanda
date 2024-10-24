load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "aws",
    srcs = ["aws.go"],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/aws",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cloud/provider",
        "@com_github_aws_aws_sdk_go//aws/ec2metadata",
        "@com_github_aws_aws_sdk_go//aws/session",
    ],
)
