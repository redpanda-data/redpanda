load("//bazel:build.bzl", "redpanda_cc_library")

package(
    default_visibility = [
        "//src/v/schema:__pkg__",
    ],
)

redpanda_cc_library(
    name = "registry",
    srcs = [
        "registry.cc",
    ],
    hdrs = [
        "registry.h",
    ],
    include_prefix = "schema",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/pandaproxy",
        "@seastar",
    ],
)
