"""
This module contains functions for building Redpanda tests. Prefer using the
methods in this module (e.g. redpanda_cc_gtest) over native Bazel functions
(e.g. cc_test) because it provides a centralized place for making behavior
changes. For example, redpanda_cc_gtest will automatically configure Seastar for
running tests, like setting a reasonable number of cores and amount of memory.
"""

load(":internal.bzl", "redpanda_copts")

def has_flags(args, *flags):
    """
    Check if flags are present in a set of arguments.

    Args:
      args: a list of command line argument strings.
      *flags: a list of flags (prefixes) to check.

    Returns:
      True if at least one flag is contained in args, and False otherwise.
    """
    for arg in args:
        for flag in flags:
            if arg.startswith(flag):
                return True
    return False

# TODO(bazel)
# - Make log level configurable (e.g. CI)
# - Set --overprovisioned in CI context
# - Other ASAN settings used in cmake_test.py
def _redpanda_cc_test(
        name,
        timeout,
        dash_dash_protocol,
        srcs = [],
        deps = [],
        default_memory_gb = None,
        default_cores = None,
        extra_args = [],
        custom_args = [],
        tags = [],
        env = {},
        data = []):
    """
    Helper to define a Redpanda C++ test.

    Args:
      name: name of the test
      timeout: same as native cc_test
      dash_dash_protocol: false for google test, true for boost test
      srcs: test source files
      deps: test dependencies
      default_memory_gb: default seastar memory
      default_cores: default seastar cores
      extra_args: arguments from test wrappers
      custom_args: arguments from cc_test users
      tags: tags to attach to the cc_test target
      env: environment variables
      data: data file dependencies
    """
    common_args = [
        "--blocked-reactor-notify-ms 2000000",
    ]

    args = common_args + extra_args + custom_args

    # Unit tests should never need all of a node's memory. Unless an explicit
    # size was requested, set a reasonable fixed value.
    if default_memory_gb and not has_flags(args, "-m", "--memory"):
        args.append("-m{}G".format(default_memory_gb))

    # Use a fixed core count unless an explicit number of cores was requested.
    # This can help (some what) with determinism across different node shapes.
    # Additionally, using a smaller value can help speed up indvidiual tests as
    # well as when multiple tests are running in parallel.
    if default_cores and not has_flags(args, "-c", "--smp"):
        args.append("-c{}".format(default_cores))

    # Google test / benchmarks don't understand the "--" protocol
    if args and dash_dash_protocol:
        args = ["--"] + args

    native.cc_test(
        name = name,
        timeout = timeout,
        srcs = srcs,
        deps = deps,
        copts = redpanda_copts(),
        args = args,
        features = [
            "layering_check",
        ],
        tags = tags,
        env = env,
        data = data,
    )

def _redpanda_cc_unit_test(**kwargs):
    extra_args = [
        "--unsafe-bypass-fsync 1",
        "--default-log-level=trace",
        "--logger-log-level='io=debug'",
        "--logger-log-level='exception=debug'",
    ]
    _redpanda_cc_test(
        default_memory_gb = 1,
        default_cores = 4,
        extra_args = extra_args,
        **kwargs
    )

def redpanda_cc_gtest(
        name,
        timeout,
        srcs = [],
        deps = [],
        args = []):
    _redpanda_cc_unit_test(
        dash_dash_protocol = False,
        name = name,
        timeout = timeout,
        srcs = srcs,
        deps = deps,
        custom_args = args,
    )

def redpanda_cc_btest(
        name,
        timeout,
        srcs = [],
        deps = [],
        args = [],
        env = {},
        data = []):
    _redpanda_cc_unit_test(
        dash_dash_protocol = True,
        name = name,
        timeout = timeout,
        srcs = srcs,
        deps = deps,
        custom_args = args,
        env = env,
        data = data,
    )

def redpanda_cc_bench(
        name,
        timeout,
        srcs = [],
        deps = [],
        args = []):
    _redpanda_cc_test(
        dash_dash_protocol = False,
        default_cores = 1,
        name = name,
        timeout = timeout,
        srcs = srcs,
        deps = deps,
        custom_args = args,
        tags = [
            "bench",
        ],
    )

def redpanda_cc_btest_no_seastar(
        name,
        timeout,
        srcs = [],
        defines = [],
        deps = []):
    native.cc_test(
        name = name,
        timeout = timeout,
        srcs = srcs,
        defines = defines,
        deps = ["@boost//:test.so"] + deps,
    )

def redpanda_test_cc_library(
        name,
        srcs = [],
        hdrs = [],
        defines = [],
        local_defines = [],
        visibility = None,
        include_prefix = None,
        implementation_deps = [],
        deps = []):
    native.cc_library(
        name = name,
        srcs = srcs,
        hdrs = hdrs,
        defines = defines,
        local_defines = local_defines,
        visibility = visibility,
        include_prefix = include_prefix,
        implementation_deps = implementation_deps,
        deps = deps,
        copts = redpanda_copts(),
        testonly = True,
    )
