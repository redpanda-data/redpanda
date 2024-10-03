load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "base",
    hdrs = [
        "likely.h",
        "oncore.h",
        "outcome.h",
        "outcome_future_utils.h",
        "seastarx.h",
        "source_location.h",
        "type_traits.h",
        "units.h",
        "unreachable.h",
        "vassert.h",
        "vformat.h",
        "vlog.h",
    ],
    include_prefix = "base",
    visibility = ["//visibility:public"],
    deps = [
        "@boost//:outcome",
        "@fmt",
        "@seastar",
    ],
)
