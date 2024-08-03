load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "base",
    hdrs = [
        "include/base/likely.h",
        "include/base/oncore.h",
        "include/base/outcome.h",
        "include/base/outcome_future_utils.h",
        "include/base/seastarx.h",
        "include/base/source_location.h",
        "include/base/type_traits.h",
        "include/base/units.h",
        "include/base/vassert.h",
        "include/base/vformat.h",
        "include/base/vlog.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "@boost//:outcome",
        "@fmt",
        "@seastar",
    ],
)
