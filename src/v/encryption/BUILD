load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "encryption",
    srcs = [
        "mock_kms_provider.cc",
    ],
    hdrs = [
        "fwd.h",
        "kms_provider.h",
        "mock_kms_provider.h",
        "types.h",
    ],
    implementation_deps = [
        "@openssl",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/bytes",
        "//src/v/container:chunked_hash_map",
        "//src/v/crypto",
        "//src/v/model",
        "@abseil-cpp//absl/hash",
        "@seastar",
    ],
)

