load("@rules_go//go:def.bzl", "go_library")

go_library(
    name = "publicapi",
    srcs = [
        "controlplane.go",
        "dataplane.go",
        "publicapi.go",
        "transform.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/httpapi",
        "@build_buf_gen_go_redpandadata_cloud_connectrpc_go//redpanda/api/controlplane/v1beta2/controlplanev1beta2connect",
        "@build_buf_gen_go_redpandadata_cloud_protocolbuffers_go//redpanda/api/controlplane/v1beta2",
        "@build_buf_gen_go_redpandadata_common_protocolbuffers_go//redpanda/api/common/v1alpha1",
        "@build_buf_gen_go_redpandadata_dataplane_connectrpc_go//redpanda/api/dataplane/v1alpha1/dataplanev1alpha1connect",
        "@build_buf_gen_go_redpandadata_dataplane_protocolbuffers_go//redpanda/api/dataplane/v1alpha1",
        "@com_connectrpc_connect//:connect",
        "@org_uber_go_zap//:zap",
    ],
)
