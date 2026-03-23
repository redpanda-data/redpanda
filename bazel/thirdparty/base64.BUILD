load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

# TODO(bazel)
# - Verify architecture specific settings
# - Verify that platform select works as expected

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

common_cache_entries = {
    "BUILD_SHARED_LIBS": "OFF",
    "CMAKE_INSTALL_LIBDIR": "lib",
    "BASE64_WITH_OpenMP": "OFF",
    "BASE64_WERROR": "OFF",
    # Fix RPATH issue with Ninja generator during cross-compilation
    # See: https://cmake.org/cmake/help/latest/variable/CMAKE_BUILD_WITH_INSTALL_RPATH.html
    "CMAKE_BUILD_WITH_INSTALL_RPATH": "ON",
}

cmake(
    name = "base64",
    # Only build for Linux target - avoid cross-compilation issues on darwin exec
    target_compatible_with = ["@platforms//os:linux"],
    cache_entries = common_cache_entries | select({
        "@platforms//cpu:x86_64": {
            "BASE64_WITH_SSSE3": "ON",
            "BASE64_WITH_SSE41": "ON",
            "BASE64_WITH_SSE42": "ON",
            "BASE64_WITH_AVX": "OFF",
            "BASE64_WITH_AVX2": "OFF",
            "BASE64_WITH_NEON32": "OFF",
            "BASE64_WITH_NEON64": "OFF",
        },
        "@platforms//cpu:arm64": {
            "BASE64_WITH_SSSE3": "OFF",
            "BASE64_WITH_SSE41": "OFF",
            "BASE64_WITH_SSE42": "OFF",
            "BASE64_WITH_AVX": "OFF",
            "BASE64_WITH_AVX2": "OFF",
            "BASE64_WITH_NEON32": "OFF",
            "BASE64_WITH_NEON64": "ON",
        },
    }),
    generate_args = ["-GNinja"],
    lib_source = ":srcs",
    out_static_libs = ["libbase64.a"],
    visibility = [
        "//visibility:public",
    ],
)
