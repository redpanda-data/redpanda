load("//bazel:build.bzl", "redpanda_cc_btest_no_seastar", "redpanda_cc_library")

redpanda_cc_library(
    name = "utf8",
    srcs = [
        "utf8.cc",
    ],
    hdrs = [
        "include/strings/utf8.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@boost//:locale",
    ],
)

redpanda_cc_library(
    name = "string_switch",
    hdrs = [
        "include/strings/string_switch.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
)

redpanda_cc_btest_no_seastar(
    name = "strings_test",
    timeout = "short",
    srcs = [
        "tests/constexpr_string_switch.cc",
        "tests/string_switch_test.cc",
        "tests/utf8_control_chars.cc",
    ],
    deps = [
        ":string_switch",
        ":utf8",
    ],
)
