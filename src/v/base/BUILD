load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "base",
    srcs = [
        "vassert.cc",
    ],
    hdrs = [
        "compiler_utils.h",
        "format_to.h",
        "invariants.h",
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
    defines = select({
        "//bazel:antithesis_enabled": ["REDPANDA_INVARIANTS_ANTITHESIS"],
        "//conditions:default": [],
    }),
    visibility = ["//visibility:public"],
    deps = [
        "@boost//:outcome",
        "@fmt",
        "@seastar",
    ] + select({
        "//bazel:antithesis_enabled": ["@antithesis_sdk_cpp//:antithesis_sdk"],
        "//conditions:default": [],
    }),
)
