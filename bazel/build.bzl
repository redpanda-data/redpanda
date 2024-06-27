"""
This module contains functions for building Redpanda libraries and executables.
Prefer using the methods in this module (e.g. redpanda_cc_library) over native
Bazel functions (e.g. cc_library) because it provides a centralized place for
making behavior changes across the entire build.
"""

load(":internal.bzl", "redpanda_copts")

def redpanda_cc_library(
        name,
        srcs = [],
        hdrs = [],
        defines = [],
        strip_include_prefix = None,
        visibility = None,
        include_prefix = None,
        deps = []):
    """
    Define a Redpanda C++ library.
    """
    native.cc_library(
        name = name,
        srcs = srcs,
        hdrs = hdrs,
        defines = defines,
        visibility = visibility,
        include_prefix = include_prefix,
        strip_include_prefix = strip_include_prefix,
        deps = deps,
        copts = redpanda_copts(),
    )
