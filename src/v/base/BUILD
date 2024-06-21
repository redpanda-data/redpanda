load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_cc_btest_no_seastar")

redpanda_cc_library(
    name = "base",
    hdrs = [
        "include/base/likely.h",
        "include/base/oncore.h",
        "include/base/outcome.h",
        "include/base/seastarx.h",
        "include/base/source_location.h",
        "include/base/type_traits.h",
        "include/base/units.h",
        "include/base/vassert.h",
        "include/base/vlog.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "@boost//:outcome",
        "@seastar",
    ],
)

redpanda_cc_btest_no_seastar(
    name = "outcome_test",
    timeout = "short",
    srcs = [
        "tests/outcome_test.cc",
    ],
    deps = [
        ":base",
    ],
)
