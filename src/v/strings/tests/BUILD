load("//bazel:test.bzl", "redpanda_cc_btest_no_seastar", "redpanda_cc_gtest")

redpanda_cc_btest_no_seastar(
    name = "strings_test",
    timeout = "short",
    srcs = [
        "constexpr_string_switch.cc",
        "static_str_test.cc",
        "utf8_control_chars.cc",
        "utf8_substring.cc",
    ],
    deps = [
        "//src/v/strings:static_str",
        "//src/v/strings:string_switch",
        "//src/v/strings:utf8",
    ],
)

redpanda_cc_gtest(
    name = "string_switch_gtest",
    timeout = "short",
    srcs = [
        "string_switch_test.cc",
    ],
    deps = [
        "//src/v/strings:string_switch",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
    ],
)
