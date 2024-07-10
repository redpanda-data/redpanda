load("//bazel:test.bzl", "redpanda_cc_btest_no_seastar")

redpanda_cc_btest_no_seastar(
    name = "strings_test",
    timeout = "short",
    srcs = [
        "constexpr_string_switch.cc",
        "string_switch_test.cc",
        "utf8_control_chars.cc",
    ],
    deps = [
        "//src/v/strings:string_switch",
        "//src/v/strings:utf8",
    ],
)
