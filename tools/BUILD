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

#
# When running the dev_cluster the default data path is the working directory
# where the python script is run. Bazel runs the commands in a runfiles
# directory which makes it inconvenient to interact with the cluster data
# directory.
#
# When running you can pass in `-d` so that the data directory will be your
# current working directory.
#
#     bazel run //tools:dev_cluster -- --nodes 1 -d ${PWD}/data
#
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

sh_binary(
    name = "clang_format",
    srcs = ["format_cc.sh"],
    args = [
        "$(rootpath @llvm_18_toolchain//:clang-format)",
    ],
    data = [
        "@llvm_18_toolchain//:clang-format",
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
