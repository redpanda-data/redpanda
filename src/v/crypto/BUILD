load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "crypto",
    srcs = [
        "crypto.cc",
        "digest.cc",
        "hmac.cc",
        "internal.h",
        "key.cc",
        "key.h",
        "ossl_context_service.cc",
        "random.cc",
        "signature.cc",
        "ssl_utils.cc",
        "ssl_utils.h",
    ],
    hdrs = [
        "include/crypto/crypto.h",
        "include/crypto/exceptions.h",
        "include/crypto/ossl_context_service.h",
        "include/crypto/types.h",
    ],
    strip_include_prefix = "include",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes",
        "//src/v/ssx:thread_worker",
        "@abseil-cpp//absl/container:node_hash_map",
        "@openssl",
    ],
)
