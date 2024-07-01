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

    # TODO Bazel prefers -iquote "path" style includes in many cases. However,
    # our source tree uses bracket <path> style for dependencies. We need a way
    # to bridge this gap until we decide to fully switch over to Bazel at which
    # point this hack can be removed. Many deps lists in the tree will probably
    # need to be updated to include abseil explicitly when this is removed.
    copts = []
    copts.append("-Iexternal/abseil-cpp~")

    copts.append("-Werror")
    copts.append("-Wall")
    copts.append("-Wextra")
    copts.append("-Wno-missing-field-initializers")
    copts.append("-Wimplicit-fallthrough")

    return copts
