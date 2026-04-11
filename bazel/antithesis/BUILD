load("@rules_cc//cc:cc_library.bzl", "cc_library")

cc_library(
    name = "instrumentation",
    srcs = ["antithesis_instrumentation.cc"],
    visibility = ["//visibility:public"],
    deps = ["@antithesis_sdk_cpp//:antithesis_sdk"],
    alwayslink = True,
)
