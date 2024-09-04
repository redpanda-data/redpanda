load("@buildifier_prebuilt//:rules.bzl", "buildifier")
load("@rules_python//python:defs.bzl", "py_binary")

buildifier(
    name = "buildifier.check",
    exclude_patterns = [
        "./.git/*",
        "./build/*",
        "./vbuild/*",
    ],
    lint_mode = "warn",
    mode = "diff",
)

buildifier(
    name = "buildifier.fix",
    exclude_patterns = [
        "./.git/*",
        "./build/*",
        "./vbuild/*",
    ],
    lint_mode = "warn",
    mode = "fix",
)

py_binary(
    name = "dev_cluster",
    srcs = [
        "dev_cluster.py",
    ],
    args = [
        "-e $(rootpath //src/v/redpanda:redpanda)",
    ],
    data = [
        "//src/v/redpanda",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@python_deps//psutil",
        "@python_deps//pyyaml",
    ],
)
