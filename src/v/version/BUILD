load("@bazel_skylib//rules:expand_template.bzl", "expand_template")
load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("//bazel:build.bzl", "redpanda_cc_library")
load(":expand_with_stamp_vars.bzl", "expand_with_stamp_vars")

# Our python binary used to parse the workspace_status output and expand a template with those variables.
py_binary(
    name = "stamper",
    srcs = ["stamper.py"],
)

# These are our defaults, it uses the same format as the values printed in //bazel:workspace_status.sh
# If not using --config=stamp, then these will be the values in version.cc
write_file(
    name = "nostamp_variables",
    out = "nostamp_variables.txt",
    content = [
        "STABLE_GIT_LATEST_TAG 0.0.0-dev",
        "STABLE_GIT_COMMIT 0000000000000000000000000000000000000000",
        "STABLE_GIT_TREE_DIRTY ",
    ],
)

# Convert from a CMake template, to a template that works with Python's built-in string.Template
expand_template(
    name = "version_template",
    out = "version.in.bazel.cc",
    substitutions = {
        "@GIT_VER@": "${STABLE_GIT_LATEST_TAG}",
        "@GIT_SHA1@": "${STABLE_GIT_COMMIT}",
        "@GIT_CLEAN_DIRTY@": "${STABLE_GIT_TREE_DIRTY}",
    },
    template = "version.cc.in",
)

# Use our custom rule to inject the variables from workspace_status.sh or our default if the
# workspace_status wasn't used.
expand_with_stamp_vars(
    name = "version_cc",
    out = "version.cc",
    defaults_file = ":nostamp_variables",
    template = ":version_template",
)

redpanda_cc_library(
    name = "version",
    srcs = [
        ":version_cc",
    ],
    hdrs = [
        "version.h",
    ],
    include_prefix = "version",
    visibility = ["//visibility:public"],
)
