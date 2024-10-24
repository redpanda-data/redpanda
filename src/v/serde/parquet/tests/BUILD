load("@bazel_skylib//rules:run_binary.bzl", "run_binary")
load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("//bazel:build.bzl", "redpanda_cc_binary")
load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "value_test",
    timeout = "short",
    srcs = [
        "value_test.cc",
    ],
    cpu = 1,
    memory = "64MiB",
    deps = [
        "//src/v/serde/parquet:value",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_binary(
    name = "generate_metadata_binary",
    srcs = ["generate_metadata.cc"],
    deps = [
        "//src/v/base",
        "//src/v/bytes:iostream",
        "//src/v/serde/parquet:metadata",
        "//src/v/serde/parquet:schema",
        "@seastar",
    ],
)

write_file(
    name = "test_tool_wrapper",
    out = "test_tool_wrapper.sh",
    content = [
        "#!/bin/bash",
        "exec $@",
    ],
)

TEST_CASES = range(4)

[
    (
        run_binary(
            name = "generated_metadata_{}".format(test_case),
            outs = ["metadata_test_case_{}_output.thrift".format(test_case)],
            args = [
                "--output",
                "$(execpath :metadata_test_case_{}_output.thrift)".format(test_case),
                "--test-case",
                "{}".format(test_case),
            ],
            tool = ":generate_metadata_binary",
        ),
        sh_test(
            name = "verify_metadata_{}".format(test_case),
            size = "small",
            timeout = "short",
            srcs = [":test_tool_wrapper"],
            args = [
                "$(rootpath //src/v/test_utils/go/parquet_metadata_verifier)",
                "-test-case={}".format(test_case),
                "-input-file=$(rootpath :generated_metadata_{})".format(test_case),
            ],
            data = [
                ":generated_metadata_{}".format(test_case),
                "//src/v/test_utils/go/parquet_metadata_verifier",
            ],
        ),
    )
    for test_case in TEST_CASES
]

redpanda_cc_gtest(
    name = "encoding_test",
    timeout = "short",
    srcs = [
        "encoding_test.cc",
    ],
    cpu = 1,
    memory = "64MiB",
    deps = [
        "//src/v/serde/parquet:encoding",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
