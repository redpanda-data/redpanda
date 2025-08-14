load("@bazel_skylib//rules:native_binary.bzl", "native_binary")
load("@buildifier_prebuilt//:rules.bzl", "buildifier")
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

native_binary(
    name = "minio",
    src = "@minio//file",
)

# Run a dev_cluster, by default the data directory is in the repository root.
#
#     bazel run //tools:dev_cluster -- --nodes 1
#
# To specify cluster configuration:
#
#     bazel run //tools:dev_cluster -- --nodes 1 --no-use-minio --config-overrides '{"cloud_storage_enabled": true, "cloud_storage_secret_key": "<secret_key>", "cloud_storage_access_key": "<access_key>", "cloud_storage_region": "us-west-2", "cloud_storage_bucket": "my_bucket", "iceberg_enabled": true, "iceberg_catalog_type": "rest", "iceberg_rest_catalog_endpoint": "https://glue.us-west-2.amazonaws.com/iceberg", "iceberg_rest_catalog_authentication_mode": "aws_sigv4", "iceberg_rest_catalog_base_location": "s3://my_bucket/glue"}'
py_binary(
    name = "dev_cluster",
    srcs = [
        "dev_cluster.py",
    ],
    args = [
        "-e $(rootpath //src/v/redpanda:redpanda)",
        "-o $(rootpath :minio)",
        "--rpk $(rootpath //src/go/rpk/cmd/rpk)",
    ],
    data = [
        ":minio",
        "//src/go/rpk/cmd/rpk",
        "//src/v/redpanda",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@python_deps//aioboto3",
        "@python_deps//boto3",
        "@python_deps//psutil",
        "@python_deps//pyyaml",
        "@python_deps//s3transfer",
    ],
)

sh_binary(
    name = "clang_format",
    srcs = ["format_cc.sh"],
    args = [
        "$(rootpath @previous_llvm_toolchain//:clang-format)",
    ],
    data = [
        "@previous_llvm_toolchain//:clang-format",
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
