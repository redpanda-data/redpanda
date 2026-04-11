load("@rules_cc//cc:cc_library.bzl", "cc_library")

cc_library(
    name = "antithesis_sdk",
    hdrs = [
        "antithesis_instrumentation.h",
        "antithesis_sdk.h",
    ],
    includes = ["."],
    linkopts = ["-ldl"],
    visibility = ["//visibility:public"],
)
