load("@bazel_skylib//rules:native_binary.bzl", "native_binary")

filegroup(
    name = "grafana_server",
    srcs = select({
        "@platforms//cpu:x86_64": ["@grafana_amd64//:bin/grafana-server"],
        "@platforms//cpu:aarch64": ["@grafana_arm64//:bin/grafana-server"],
        "//conditions:default": ["@grafana_amd64//:bin/grafana-server"],
    }),
    visibility = ["//visibility:public"],
)

filegroup(
    name = "grafana_cli",
    srcs = select({
        "@platforms//cpu:x86_64": ["@grafana_amd64//:bin/grafana-cli"],
        "@platforms//cpu:aarch64": ["@grafana_arm64//:bin/grafana-cli"],
        "//conditions:default": ["@grafana_amd64//:bin/grafana-cli"],
    }),
    visibility = ["//visibility:public"],
)

filegroup(
    name = "grafana_files",
    srcs = select({
        "@platforms//cpu:x86_64": ["@grafana_amd64//:conf", "@grafana_amd64//:public"],
        "@platforms//cpu:aarch64": ["@grafana_arm64//:conf", "@grafana_arm64//:public"],
        "//conditions:default": ["@grafana_amd64//:conf", "@grafana_amd64//:public"],
    }),
    visibility = ["//visibility:public"],
)

native_binary(
    name = "grafana",
    src = ":grafana_server",
    data = [
        ":grafana_files",
    ],
    visibility = ["//visibility:public"],
)
