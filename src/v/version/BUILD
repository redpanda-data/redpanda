load("@bazel_skylib//rules:expand_template.bzl", "expand_template")
load("//bazel:build.bzl", "redpanda_cc_library")

# TODO(bazel) https://redpandadata.atlassian.net/browse/CORE-5568 there is some
# learning and bits to create to properly get the version embedded. however, we
# don't want to block all progress (there are, unfortunatley, a lot of uses in
# the tree of redpanda_version) until that is addressed. so instead we'll fill
# in fake version information in the bazel build until CORE-5568 is resolved.

expand_template(
    name = "version_cc",
    out = "version.cc",
    substitutions = {
        "@GIT_VER@": "0.0.0-dev",
        "@GIT_SHA1@": "0000000000000000000000000000000000000000",
        "@GIT_CLEAN_DIRTY@": "",
    },
    template = "version.cc.in",
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
