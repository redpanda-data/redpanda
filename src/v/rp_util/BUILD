load("//bazel:build.bzl", "redpanda_cc_binary")

redpanda_cc_binary(
    name = "rp_util",
    testonly = True,
    srcs = [
        "main.cc",
    ],
    # Silences "CFG hash mismatch" warnings which stem from the fact that we compile
    # multiple binaries with only the RP profile data
    # Similar to: https://issues.chromium.org/issues/40272404
    copts = [
        "-Wno-backend-plugin",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iostream",
        "//src/v/redpanda/admin:cluster_config_schema_util",
        "//src/v/version",
        "@boost//:program_options",
        "@seastar",
    ],
)
