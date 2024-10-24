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

def parse_bytes(value):
    """
    Convert a string into bytes number. Does not respect SI standard.

    Args:
      value: "4MiB" or "1G" or "2MB"

    Returns:
      the number of bytes representing the string
    """
    for suffix in ["iB", "B"]:
        if value.endswith(suffix):
            value = value.removesuffix(suffix)
            break
    factor = 1
    for (suffix, power) in [("G", 30), ("M", 20), ("K", 10)]:
        if value.endswith(suffix):
            factor = factor << power
            value = value.removesuffix(suffix)
            break
    return int(value) * factor

# TODO(bazel)
# - Make log level configurable (e.g. CI)
# - Set --overprovisioned in CI context
# - Other ASAN settings used in cmake_test.py
def _redpanda_cc_test(
        name,
        timeout,
        dash_dash_protocol,
        memory,
        cpu,
        srcs = [],
        deps = [],
        extra_args = [],
        custom_args = [],
        tags = [],
        env = {},
        target_compatible_with = [],
        data = [],
        local_defines = []):
    """
    Helper to define a Redpanda C++ test.

    Args:
      name: name of the test
      timeout: same as native cc_test
      dash_dash_protocol: false for google test, true for boost test
      srcs: test source files
      deps: test dependencies
      memory: seastar memory as a string ("1GB" or "512MiB" or "256M")
      cpu: seastar cores
      extra_args: arguments from test wrappers
      custom_args: arguments from cc_test users
      tags: tags to attach to the cc_test target
      env: environment variables
      target_compatible_with: constraints
      data: data file dependencies
      local_defines: list of defines
    """
    common_args = [
        "--blocked-reactor-notify-ms 2000000",
        "--abort-on-seastar-bad-alloc",
        "--overprovisioned",
    ]

    args = common_args + extra_args + custom_args

    if has_flags(args, "-m", "--memory"):
        fail("Use `memory=\"XGiB\"` test parameter instead of -m/--memory")
    if has_flags(args, "-c", "--smp"):
        fail("Use `cpu=N` test parameter instead of -c/--smp")

    args.append("-m{}".format(memory))
    args.append("-c{}".format(cpu))
    resource_tags = [
        "resources:cpu:{}".format(cpu),
        # This is always defined in MiB for Bazel
        "resources:memory:{}".format(parse_bytes(memory) / (1 << 20)),
    ]

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
        tags = resource_tags + tags,
        env = env,
        target_compatible_with = target_compatible_with,
        data = data,
        local_defines = local_defines,
    )

def _redpanda_cc_unit_test(cpu, memory, **kwargs):
    extra_args = [
        "--unsafe-bypass-fsync 1",
        "--default-log-level=trace",
        "--logger-log-level='io=debug'",
        "--logger-log-level='exception=debug'",
    ]

    # TODO(bazel): What are the right defaults here?
    _redpanda_cc_test(
        memory = memory or "1GiB",
        cpu = cpu or 4,
        extra_args = extra_args,
        **kwargs
    )

def redpanda_cc_gtest(
        name,
        timeout,
        srcs = [],
        deps = [],
        args = [],
        env = {},
        cpu = None,
        memory = None,
        data = []):
    _redpanda_cc_unit_test(
        dash_dash_protocol = False,
        name = name,
        timeout = timeout,
        srcs = srcs,
        cpu = cpu,
        memory = memory,
        deps = deps,
        custom_args = args,
        env = env,
        data = data,
        local_defines = ["IS_GTEST"],
    )

def redpanda_cc_btest(
        name,
        timeout,
        srcs = [],
        deps = [],
        args = [],
        env = {},
        cpu = None,
        memory = None,
        target_compatible_with = [],
        data = []):
    _redpanda_cc_unit_test(
        dash_dash_protocol = True,
        name = name,
        timeout = timeout,
        srcs = srcs,
        deps = deps,
        cpu = cpu,
        memory = memory,
        custom_args = args,
        env = env,
        target_compatible_with = target_compatible_with,
        data = data,
    )

def redpanda_cc_bench(
        name,
        timeout,
        srcs = [],
        deps = [],
        args = [],
        env = {},
        cpu = None,
        memory = None,
        data = []):
    _redpanda_cc_test(
        dash_dash_protocol = False,
        cpu = cpu or 1,
        memory = memory or "1GiB",
        data = data,
        env = env,
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
        cpu = 1,
        memory = "128MiB",
        deps = []):
    native.cc_test(
        name = name,
        timeout = timeout,
        tags = [
            "resources:cpu:{}".format(cpu),
            "resources:memory:{}".format(parse_bytes(memory) / (2 << 20)),
        ],
        srcs = srcs,
        defines = defines,
        copts = redpanda_copts(),
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
