"""
This module contains functions for building Redpanda libraries and executables.
Prefer using the methods in this module (e.g. redpanda_cc_library) over native
Bazel functions (e.g. cc_library) because it provides a centralized place for
making behavior changes across the entire build.
"""

load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load(":internal.bzl", "antithesis_deps", "redpanda_copts")

# buildifier: disable=function-docstring-args
def redpanda_cc_library(
        name,
        srcs = [],
        hdrs = [],
        defines = [],
        local_defines = [],
        implementation_deps = [],
        include_prefix = None,
        visibility = None,
        copts = [],
        deps = [],
        tags = []):
    """
    Define a Redpanda C++ library.
    """
    if include_prefix == None:
        include_prefix = native.package_name().removeprefix("src/v/")
    cc_library(
        name = name,
        srcs = srcs,
        hdrs = hdrs,
        defines = defines,
        local_defines = local_defines,
        visibility = visibility,
        include_prefix = include_prefix,
        implementation_deps = implementation_deps,
        deps = deps,
        copts = redpanda_copts() + copts,
        tags = tags,
        features = [
            "layering_check",
        ],
    )

# buildifier: disable=function-docstring-args
def redpanda_cc_binary(
        name,
        srcs = [],
        defines = [],
        local_defines = [],
        visibility = None,
        testonly = False,
        copts = [],
        linkopts = [],
        deps = []):
    """
    Define a Redpanda C++ binary.
    """
    cc_binary(
        name = name,
        srcs = srcs,
        defines = defines,
        local_defines = local_defines,
        visibility = visibility,
        deps = deps + antithesis_deps(),
        testonly = testonly,
        copts = redpanda_copts() + copts,
        linkopts = linkopts,
        features = [
            "layering_check",
        ],
    )
