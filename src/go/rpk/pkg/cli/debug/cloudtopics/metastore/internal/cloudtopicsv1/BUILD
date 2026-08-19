load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "cloudtopicsv1",
    srcs = [
        "doc.go",
        "metastore.pb.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/cloudtopics/metastore/internal/cloudtopicsv1",
    visibility = ["//src/go/rpk/pkg/cli/debug/cloudtopics/metastore:__subpackages__"],
    deps = [
        "@build_buf_gen_go_redpandadata_core_protocolbuffers_go//redpanda/core/common/v1:common",
        "@build_buf_gen_go_redpandadata_core_protocolbuffers_go//redpanda/core/pbgen",
        "@org_golang_google_protobuf//reflect/protoreflect",
        "@org_golang_google_protobuf//runtime/protoimpl",
    ],
)
