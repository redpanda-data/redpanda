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
