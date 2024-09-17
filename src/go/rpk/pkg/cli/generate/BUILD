load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "generate",
    srcs = [
        "app.go",
        "autocomplete.go",
        "generate.go",
        "grafana.go",
        "prometheus.go",
    ],
    embedsrcs = [
        "app-templates/go-produce-consume.tar.gz",
        "grafana-dashboards/Kafka-Consumer-Metrics.json.gz",
        "grafana-dashboards/Kafka-Consumer-Offsets.json.gz",
        "grafana-dashboards/Kafka-Topic-Metrics.json.gz",
        "grafana-dashboards/Redpanda-Ops-Dashboard.json.gz",
    ],
    importpath = "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/generate",
    visibility = ["//visibility:public"],
    deps = [
        "//src/go/rpk/pkg/adminapi",
        "//src/go/rpk/pkg/cli/generate/graf",
        "//src/go/rpk/pkg/config",
        "//src/go/rpk/pkg/httpapi",
        "//src/go/rpk/pkg/kafka",
        "//src/go/rpk/pkg/os",
        "//src/go/rpk/pkg/out",
        "@com_github_prometheus_client_model//go",
        "@com_github_prometheus_common//expfmt",
        "@com_github_spf13_afero//:afero",
        "@com_github_spf13_cobra//:cobra",
        "@com_github_spf13_pflag//:pflag",
        "@com_github_twmb_franz_go//pkg/kgo",
        "@com_github_twmb_franz_go_pkg_kadm//:kadm",
        "@in_gopkg_yaml_v3//:yaml_v3",
        "@org_uber_go_zap//:zap",
    ],
)

go_test(
    name = "generate_test",
    size = "small",
    srcs = [
        "app_test.go",
        "grafana_test.go",
        "prometheus_test.go",
    ],
    embed = [":generate"],
    deps = [
        "//src/go/rpk/pkg/config",
        "@com_github_spf13_afero//:afero",
        "@com_github_stretchr_testify//require",
    ],
)
