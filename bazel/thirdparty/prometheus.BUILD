load("@bazel_skylib//rules:native_binary.bzl", "native_binary")

filegroup(
    name = "prometheus_bin",
    srcs = select({
        "@platforms//cpu:x86_64": ["@prometheus_amd64//:prom/prometheus"],
        "@platforms//cpu:aarch64": ["@prometheus_arm64//:prom/prometheus"],
        "//conditions:default": ["@prometheus_amd64//:prom/prometheus"],
    }),
    visibility = ["//visibility:public"],
)

filegroup(
    name = "promtool",
    srcs = select({
        "@platforms//cpu:x86_64": ["@prometheus_amd64//:prom/promtool"],
        "@platforms//cpu:aarch64": ["@prometheus_arm64//:prom/promtool"],
        "//conditions:default": ["@prometheus_amd64//:prom/promtool"],
    }),
    visibility = ["//visibility:public"],
)

filegroup(
    name = "prometheus_config",
    srcs = select({
        "@platforms//cpu:x86_64": ["@prometheus_amd64//:prom/prometheus.yml"],
        "@platforms//cpu:aarch64": ["@prometheus_arm64//:prom/prometheus.yml"],
        "//conditions:default": ["@prometheus_amd64//:prom/prometheus.yml"],
    }),
    visibility = ["//visibility:public"],
)

native_binary(
    name = "prometheus",
    src = ":prometheus_bin",
    data = [
        ":prometheus_config",
    ],
    visibility = ["//visibility:public"],
)
