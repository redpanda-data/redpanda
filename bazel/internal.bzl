"""
This module contains internal helpers that should not be used outside of the
scripts in the `bazel/` directory.
"""

def redpanda_copts():
    """
    Add common options to redpanda targets.

    Returns:
      Options to be added to target.
    """

    copts = []
    copts.append("-Werror")
    copts.append("-Wall")
    copts.append("-Wextra")
    copts.append("-Wno-missing-field-initializers")
    copts.append("-Wimplicit-fallthrough")
    copts.append("-include")
    copts.append("base/ptree_ban.h")

    return copts

def redpanda_implicit_deps():
    """
    Deps that must be present on every redpanda C++ target so that
    the headers force-included via redpanda_copts() are visible.
    """
    return ["//src/v/base:ptree_ban"]

def antithesis_deps():
    """Conditional deps for Antithesis coverage instrumentation."""
    return select({
        "//bazel:antithesis_enabled": ["//bazel/antithesis:instrumentation"],
        "//conditions:default": [],
    })

def _filtered_filegroup_impl(ctx):
    return [DefaultInfo(files = depset([
        f
        for src in ctx.attr.srcs
        for f in src[DefaultInfo].files.to_list()
        if any([include in f.path for include in ctx.attr.path_includes])
    ]))]

filtered_filegroup = rule(
    implementation = _filtered_filegroup_impl,
    attrs = {
        "path_includes": attr.string_list(mandatory = True),
        "srcs": attr.label_list(allow_files = True, mandatory = True),
    },
)
