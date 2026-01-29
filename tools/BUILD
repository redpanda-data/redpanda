load("@buildifier_prebuilt//:rules.bzl", "buildifier")
load("@rules_pkg//pkg:tar.bzl", "pkg_tar")
load("@rules_python//python:defs.bzl", "py_binary")
load("@rules_shell//shell:sh_binary.bzl", "sh_binary")

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
    lint_mode = "fix",
    mode = "fix",
)

DEV_CLUSTER_PYTHON_DEPS = [
    "@python_deps//aioboto3",
    "@python_deps//boto3",
    "@python_deps//psutil",
    "@python_deps//pyyaml",
    "@python_deps//s3transfer",
]

# Run a dev_cluster, by default the data directory is in the repository root.
#
#     bazel run //tools:dev_cluster -- --nodes 1
#
# To specify cluster configuration:
#
#     bazel run //tools:dev_cluster -- --nodes 1 --no-use-minio --config-overrides '{"cloud_storage_enabled": true, "cloud_storage_secret_key": "<secret_key>", "cloud_storage_access_key": "<access_key>", "cloud_storage_region": "us-west-2", "cloud_storage_bucket": "my_bucket", "iceberg_enabled": true, "iceberg_catalog_type": "rest", "iceberg_rest_catalog_endpoint": "https://glue.us-west-2.amazonaws.com/iceberg", "iceberg_rest_catalog_authentication_mode": "aws_sigv4", "iceberg_rest_catalog_base_location": "s3://my_bucket/glue"}'
py_binary(
    name = "dev_cluster",
    testonly = True,
    srcs = [
        "dev_cluster.py",
    ],
    args = [
        "--executable $(rootpath //src/v/redpanda:redpanda)",
        "--rpk $(rootpath //src/go/rpk/cmd/rpk)",
        "--minio_executable $(rootpath //bazel/thirdparty:minio)",
        "--prometheus $(rootpath //bazel/thirdparty:prometheus)",
        "--grafana $(rootpath //bazel/thirdparty:grafana)",
        "--lsan_suppression_file $(rootpath //:lsan_suppressions)",
        "--ubsan_suppression_file $(rootpath //:ubsan_suppressions)",
    ],
    data = [
        "//:lsan_suppressions",
        "//:ubsan_suppressions",
        "//bazel/thirdparty:grafana",
        "//bazel/thirdparty:minio",
        "//bazel/thirdparty:prometheus",
        "//src/go/rpk/cmd/rpk",
        "//src/v/redpanda",
    ],
    visibility = ["//visibility:public"],
    deps = DEV_CLUSTER_PYTHON_DEPS,
)

# Package redpanda binary into a tarball alongside with all needed runfiles. We
# can't make a redpanda a "tool" dependency as otherwise it will run under
# "host-config"
pkg_tar(
    name = "redpanda_package_for_pgo",
    srcs = ["//src/v/redpanda"],
    extension = "tar",
    include_runfiles = True,
    package_dir = "redpanda",
    strip_prefix = "/",
    tags = ["manual"],
    visibility = ["//visibility:private"],
)

# pgo training script used as a tool target which also sets up the python deps
# for dev_cluster.py such that we can run dev_cluster from within train_pgo
py_binary(
    name = "train_pgo",
    testonly = True,
    srcs = [
        "pgo_bolt/train_pgo.py",
    ],
    data = [
        "dev_cluster.py",
    ],
    visibility = ["//visibility:private"],
    deps = DEV_CLUSTER_PYTHON_DEPS,
)

genrule(
    name = "pgo_profile",
    testonly = True,
    srcs = [
        "//bazel/thirdparty:omb_minimal",
        "//bazel/thirdparty:minio",
        "@current_llvm_toolchain//:llvm-profdata",
        "dev_cluster.py",
        ":redpanda_package_for_pgo",
        "//src/go/rpk/cmd/rpk",
    ],
    outs = ["pgo_profile.profdata"],
    cmd = "$(execpath :train_pgo) " +
          "--dev-cluster-py $(location dev_cluster.py) " +
          "--llvm-profdata-bin $(execpath @current_llvm_toolchain//:llvm-profdata) " +
          "--redpanda-tar $(location :redpanda_package_for_pgo) " +
          "--omb-benchmark $(execpath //bazel/thirdparty:omb_minimal) " +
          "--minio-binary $(execpath //bazel/thirdparty:minio) " +
          "--rpk-binary $(execpath //src/go/rpk/cmd/rpk) " +
          "--combined-profile-file $(RULEDIR)/pgo_profile.profdata",
    tags = [
        "manual",
        "supports-graceful-termination",
    ],
    tools = [
        ":train_pgo",
    ],
    visibility = ["//visibility:public"],
)

sh_binary(
    name = "clang_format",
    srcs = ["format_cc.sh"],
    args = [
        "$(rootpath @current_llvm_toolchain//:clang-format)",
    ],
    data = [
        "@current_llvm_toolchain//:clang-format",
    ],
    # Only build this target on demand, so we don't download
    # this toolchain if we're using a different one for the build.
    tags = ["manual"],
)

# Just in case you accidently mix it up
alias(
    name = "format_clang",
    actual = ":clang_format",
)

sh_binary(
    name = "clang-tidy",
    srcs = ["clang-tool.sh"],
    data = [
        "//bazel/clang_tidy/plugins:plugins.so",
    ],
    tags = ["manual"],
)

alias(
    name = "clang_tidy",
    actual = ":clang-tidy",
)
