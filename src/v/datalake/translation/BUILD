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

redpanda_cc_library(
    name = "stm",
    srcs = [
        "state_machine.cc",
    ],
    hdrs = [
        "state_machine.h",
    ],
    include_prefix = "datalake/translation",
    visibility = ["//visibility:public"],
    deps = [
        ":model",
        "//src/v/cluster",
        "//src/v/datalake:logger",
        "//src/v/raft",
        "//src/v/serde",
    ],
)
