load("//bazel:build.bzl", "redpanda_cc_library")

# TODO(bazel) https://redpandadata.atlassian.net/browse/CORE-5568 there is some
# learning and bits to create to properly get the version embedded. however, we
# don't want to block all progress (there are, unfortunatley, a lot of uses in
# the tree of redpanda_version) until that is addressed. so instead we'll fill
# in fake version information in the bazel build until CORE-5568 is resolved.

genrule(
    name = "version_cc",
    srcs = ["version.cc.in"],
    outs = ["version.cc"],
    cmd_bash = """
    sed -e 's/@GIT_VER@/1.0.0/g' \
        -e 's/@GIT_SHA1@/0000000000000000000000000000000000000000/g' \
        -e 's/@GIT_CLEAN_DIRTY@//g' $< > $@
    """,
)

redpanda_cc_library(
    name = "version",
    srcs = [
        "version.cc",
    ],
    hdrs = [
        "version.h",
    ],
    include_prefix = "version",
    visibility = ["//visibility:public"],
)
