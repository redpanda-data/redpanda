load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "publicapi",
    srcs = [
        "aigateway.go",
        "controlplane.go",
        "dataplane.go",
        "enterprise.go",
        "publicapi.go",
        "transform.go",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/cli/version",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/httpapi",
        "//src/go/rpk/pkg/oauth/authtoken",
        "@build_buf_gen_go_redpandadata_ai_gateway_connectrpc_go//redpanda/api/aigateway/v1/aigatewayv1connect",
        "@build_buf_gen_go_redpandadata_cloud_connectrpc_go//redpanda/api/byocplugin/v1alpha1/byocpluginv1alpha1connect",
        "@build_buf_gen_go_redpandadata_cloud_connectrpc_go//redpanda/api/controlplane/v1/controlplanev1connect",
        "@build_buf_gen_go_redpandadata_cloud_connectrpc_go//redpanda/api/iam/v1/iamv1connect",
        "@build_buf_gen_go_redpandadata_cloud_protocolbuffers_go//redpanda/api/byocplugin/v1alpha1",
        "@build_buf_gen_go_redpandadata_cloud_protocolbuffers_go//redpanda/api/controlplane/v1:controlplane",
        "@build_buf_gen_go_redpandadata_cloud_protocolbuffers_go//redpanda/api/iam/v1:iam",
        "@build_buf_gen_go_redpandadata_common_protocolbuffers_go//redpanda/api/common/v1:common",
        "@build_buf_gen_go_redpandadata_dataplane_connectrpc_go//redpanda/api/dataplane/v1/dataplanev1connect",
        "@build_buf_gen_go_redpandadata_dataplane_connectrpc_go//redpanda/api/dataplane/v1alpha3/dataplanev1alpha3connect",
        "@build_buf_gen_go_redpandadata_dataplane_protocolbuffers_go//redpanda/api/dataplane/v1:dataplane",
        "@build_buf_gen_go_redpandadata_gatekeeper_connectrpc_go//redpanda/api/gatekeeper/v1alpha1/gatekeeperv1alpha1connect",
        "@com_connectrpc_connect//:connect",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "publicapi_test",
    srcs = ["controlplane_test.go"],
    embed = [":publicapi"],
    deps = ["@com_github_stretchr_testify//require"],
)
