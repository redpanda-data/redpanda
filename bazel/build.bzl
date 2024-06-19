# TODO Bazel prefers -iquote "path" style includes in many cases. However, our
# source tree uses bracket <path> style for dependencies. We need a way to
# bridge this gap until we decide to fully switch over to Bazel at which point
# this hack can be removed. Many deps lists in the tree will probably need to be
# updated to include abseil explicitly when this is removed.
def _inject_copt_includes(deps):
    copts = []
    copts.append("-Iexternal/abseil-cpp~")
    return copts

def redpanda_cc_library(
        name,
        srcs = [],
        hdrs = [],
        strip_include_prefix = None,
        visibility = None,
        include_prefix = None,
        deps = []):
    native.cc_library(
        name = name,
        srcs = srcs,
        hdrs = hdrs,
        visibility = visibility,
        include_prefix = include_prefix,
        strip_include_prefix = strip_include_prefix,
        deps = deps,
        copts = _inject_copt_includes(deps),
    )

def has_flags(args, *flags):
    for arg in args:
        for flag in flags:
            if arg.startswith(flags):
                return True
    return False

# TODO
# - Rename to reflect that this is a seastar unit test
# - Make log level configurable (e.g. CI)
# - Set --overprovisioned in CI context
def _redpanda_cc_test(
        name,
        timeout,
        dash_dash_protocol,
        srcs = [],
        deps = [],
        default_memory_gb = None,
        default_cores = None,
        # from test wrappers
        extra_args = [],
        # from test author
        custom_args = []):
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
        copts = _inject_copt_includes(deps),
        args = args,
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
        args = []):
    _redpanda_cc_unit_test(
        dash_dash_protocol = True,
        name = name,
        timeout = timeout,
        srcs = srcs,
        deps = deps,
        custom_args = args,
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
    )

def redpanda_cc_btest_no_seastar(
        name,
        timeout,
        srcs = [],
        deps = []):
    native.cc_test(
        name = name,
        timeout = timeout,
        srcs = srcs,
        deps = ["@boost//:test.so"] + deps,
    )
