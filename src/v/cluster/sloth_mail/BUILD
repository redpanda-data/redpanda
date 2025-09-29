load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "sloth_mail",
    srcs = [
        "backend.cc",
    ],
    hdrs = [
        "backend.h",
        "config.h",
        "kinds.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/cluster/sloth_mail/impl",
        "//src/v/container:chunked_vector",
        "//src/v/serde",
        "@seastar",
    ],
)
