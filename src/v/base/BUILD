load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("//bazel:build.bzl", "redpanda_cc_library")

cc_library(
    name = "ptree_ban",
    hdrs = ["ptree_ban.h"],
    include_prefix = "base",
    visibility = ["//visibility:public"],
)

redpanda_cc_library(
    name = "base",
    srcs = [
        "vassert.cc",
    ],
    hdrs = [
        "compiler_utils.h",
        "external_fmt.h",
        "format_to.h",
        "likely.h",
        "oncore.h",
        "outcome.h",
        "outcome_future_utils.h",
        "seastar_fwd.h",
        "seastarx.h",
        "source_location.h",
        "type_traits.h",
        "units.h",
        "vassert.h",
        "vassert-register.h",
        "vlog.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@boost//:beast",
        "@boost//:filesystem",
        "@boost//:outcome",
        "@boost//:system",
        "@fmt",
        "@seastar",
    ],
)
