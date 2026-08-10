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
    copts.append("-Wall")
    copts.append("-Wextra")
    copts.append("-Wno-missing-field-initializers")
    copts.append("-Wimplicit-fallthrough")

    return copts

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
