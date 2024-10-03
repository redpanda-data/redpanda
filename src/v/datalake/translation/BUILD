load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "model",
    hdrs = [
        "types.h",
    ],
    include_prefix = "datalake/translation",
    visibility = [":__subpackages__"],
    deps = [
        "//src/v/model",
        "//src/v/serde",
    ],
)
