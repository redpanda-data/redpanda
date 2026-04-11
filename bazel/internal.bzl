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

    # for fmt v9 so that we do not need to write fmt::formatter wrappers for
    # every output stream operator. note that we can't move to fmt >=v10 because
    # this workaround macro was removed. so we'll need to rewrite the format
    # handling for >1000 types.
    copts.append("-DFMT_DEPRECATED_OSTREAM")

    return copts

def antithesis_deps():
    """Conditional deps for Antithesis coverage instrumentation."""
    return select({
        "//bazel:antithesis_enabled": ["//bazel/antithesis:instrumentation"],
        "//conditions:default": [],
    })
