# Platform-aware aliases for third-party tools

alias(
    name = "minio",
    actual = select({
        "@platforms//cpu:x86_64": "@minio_amd64//file",
        "@platforms//cpu:aarch64": "@minio_arm64//file",
        "//conditions:default": "@minio_amd64//file",
    }),
    visibility = ["//visibility:public"],
)

alias(
    name = "prometheus",
    actual = select({
        "@platforms//cpu:x86_64": "@prometheus_amd64//:prometheus",
        "@platforms//cpu:aarch64": "@prometheus_arm64//:prometheus",
        "//conditions:default": "@prometheus_amd64//:prometheus",
    }),
    visibility = ["//visibility:public"],
)

alias(
    name = "grafana",
    actual = select({
        "@platforms//cpu:x86_64": "@grafana_amd64//:grafana",
        "@platforms//cpu:aarch64": "@grafana_arm64//:grafana",
        "//conditions:default": "@grafana_amd64//:grafana",
    }),
    visibility = ["//visibility:public"],
)
