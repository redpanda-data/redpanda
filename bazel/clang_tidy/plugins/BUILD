load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("//bazel:internal.bzl", "filtered_filegroup", "redpanda_copts")

filtered_filegroup(
    name = "llvm_plugin_hdrs",
    srcs = ["@current_llvm_toolchain_llvm//:all_includes"],
    path_includes = [
        "/include/clang/",
        "/include/clang-c/",
        "/include/clang-tidy/",
        "/include/llvm/",
        "/include/llvm-c/",
    ],
)

# Convenience wrapper around a filegroup to satisfy layering checks.
cc_library(
    name = "libclang_hdrs",
    hdrs = [":llvm_plugin_hdrs"],
    tags = ["no-clang-tidy-headers"],
    visibility = ["//visibility:public"],
)

cc_binary(
    name = "plugins.so",
    srcs = [
        "redpanda_lambda_coroutine_deduces_this_check.cc",
        "redpanda_lambda_coroutine_deduces_this_check.h",
        "redpanda_noop_check.h",
        "redpanda_tidy_module.cc",
    ],
    copts = redpanda_copts() + [
        "-isystemexternal/toolchains_llvm++llvm+current_llvm_toolchain_llvm/include",
        "-Wno-deprecated-declarations",
        "-Wno-unused-parameter",
    ],
    features = [
        "layering_check",
    ],
    linkshared = True,
    visibility = [
        "//bazel/clang_tidy:__subpackages__",
        "//tools:__subpackages__",
    ],
    deps = [
        ":libclang_hdrs",
    ],
)
