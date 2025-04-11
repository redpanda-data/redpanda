"""
This module contains functions for building Redpanda tests. Prefer using the
methods in this module (e.g. redpanda_cc_gtest) over native Bazel functions
(e.g. cc_test) because it provides a centralized place for making behavior
changes. For example, redpanda_cc_gtest will automatically configure Seastar for
running tests, like setting a reasonable number of cores and amount of memory.
"""

load(":internal.bzl", "redpanda_copts")

def _has_flags(args, *flags):
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

def _parse_bytes(value):
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

def _test_options():
    data = [
        "//:ubsan_suppressions",
        "//:lsan_suppressions",
        "@llvm_18_toolchain//:llvm-symbolizer",
    ]
    env = {
        "BOOST_TEST_LOG_LEVEL": "test_suite",
        "BOOST_TEST_COLOR_OUTPUT": "0",
        "BOOST_TEST_CATCH_SYSTEM_ERRORS": "no",
        "BOOST_TEST_REPORT_LEVEL": "no",
        "BOOST_LOGGER": "HRF,test_suite",
        "ASAN_OPTIONS": "disable_coredump=0:abort_on_error=1",
        "ASAN_SYMBOLIZER_PATH": "$(rootpath @llvm_18_toolchain//:llvm-symbolizer)",
        "LSAN_OPTIONS": "suppressions=$(rootpath //:lsan_suppressions)",
        "UBSAN_OPTIONS": "halt_on_error=1:abort_on_error=1:report_error_type=1:suppressions=$(rootpath //:ubsan_suppressions)",
    }
    return data, env

def _redpanda_cc_test(
        name,
        timeout,
        dash_dash_protocol,
        memory,
        cpu,
        srcs = [],
        defines = [],
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
      defines: definitions of object-like macros
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

    if _has_flags(args, "-m", "--memory"):
        fail("Use `memory=\"XGiB\"` test parameter instead of -m/--memory")
    if _has_flags(args, "-c", "--smp"):
        fail("Use `cpu=N` test parameter instead of -c/--smp")

    args.append("-m{}".format(memory))
    args.append("-c{}".format(cpu))
    resource_tags = [
        "resources:cpu:{}".format(cpu),
        # This is always defined in MiB for Bazel
        "resources:memory:{}".format(_parse_bytes(memory) / (1 << 20)),
    ]

    # Google test / benchmarks don't understand the "--" protocol
    if args and dash_dash_protocol:
        args = ["--"] + args

    test_data, test_env = _test_options()

    native.cc_test(
        name = name,
        timeout = timeout,
        srcs = srcs,
        defines = defines,
        deps = deps,
        copts = redpanda_copts(),
        args = args,
        features = [
            "layering_check",
        ],
        tags = resource_tags + tags,
        env = {"RP_FIXTURE_ENV": "1"} | env | test_env,
        target_compatible_with = target_compatible_with,
        data = data + test_data,
        local_defines = local_defines,
    )

def _redpanda_cc_fuzz_test(
        name,
        timeout,
        srcs = [],
        defines = [],
        deps = [],
        custom_args = [],
        env = {},
        data = []):
    """
    Helper to define a Redpanda C++ fuzzing test.

    Args:
      name: name of the test
      timeout: same as native cc_test
      srcs: test source files
      defines: definitions of object-like macros
      deps: test dependencies
      custom_args: arguments from cc_test users
      env: environment variables
      data: data file dependencies
    """
    native.cc_test(
        name = name,
        timeout = timeout,
        srcs = srcs,
        defines = defines,
        deps = deps,
        copts = redpanda_copts(),
        args = custom_args,
        features = [
            "layering_check",
        ],
        tags = [
            "fuzz",
        ],
        env = env,
        data = data,
        linkopts = [
            "-fsanitize=fuzzer",
        ],
        target_compatible_with = select({
            "//bazel:enable_fuzz_testing": [],
            "//conditions:default": ["@platforms//:incompatible"],
        }),
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
        defines = [],
        deps = [],
        args = [],
        env = {},
        cpu = None,
        memory = None,
        data = [],
        tags = []):
    _redpanda_cc_unit_test(
        dash_dash_protocol = False,
        name = name,
        timeout = timeout,
        srcs = srcs,
        defines = defines,
        cpu = cpu,
        memory = memory,
        deps = deps,
        custom_args = args,
        env = env,
        data = data,
        local_defines = ["IS_GTEST"],
        tags = tags,
    )

def redpanda_cc_btest(
        name,
        timeout,
        srcs = [],
        defines = [],
        deps = [],
        args = [],
        env = {},
        cpu = None,
        memory = None,
        target_compatible_with = [],
        data = [],
        tags = []):
    _redpanda_cc_unit_test(
        dash_dash_protocol = True,
        name = name,
        timeout = timeout,
        srcs = srcs,
        defines = defines,
        deps = deps,
        cpu = cpu,
        memory = memory,
        custom_args = args,
        env = env,
        target_compatible_with = target_compatible_with,
        data = data,
        tags = tags,
    )

def redpanda_cc_fuzz_test(
        name,
        timeout,
        srcs = [],
        defines = [],
        deps = [],
        args = [],
        env = {},
        data = []):
    _redpanda_cc_fuzz_test(
        data = data,
        env = env,
        name = name,
        timeout = timeout,
        srcs = srcs,
        defines = defines,
        deps = deps,
        custom_args = args,
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
            "resources:memory:{}".format(_parse_bytes(memory) / (2 << 20)),
        ],
        srcs = srcs,
        defines = defines,
        copts = redpanda_copts(),
        deps = [
            "//src/v/test_utils:boost_result_redirect",
            "@boost//:test.so",
        ] + deps,
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
        features = [
            "layering_check",
        ],
    )

def redpanda_cc_bench(
        name,
        srcs = [],
        defines = [],
        timeout = "short",
        deps = [],
        args = [],
        env = {},
        cpu = 1,
        memory = "1GiB",
        runs = None,
        duration = None,
        data = [],
        tags = [],
        target_compatible_with = [],
        redirect_stderr = False,
        exec_in_shm = True):
    """
    Create a seastar benchmark target

    Args:
      name: the name of the target
      srcs: the cc files for the benchmark
      defines: any preprocessor defines
      deps: the dependencies for the benchmark binary
      args: any custom arguments for the binary
      env: any custom environment variables for the binary
      cpu: the number of cores the benchmark needs
      memory: the amount of RAM needed for the benchmark
      runs: number of runs or None for default, applies to run but not test
      duration: duration of a single run in seconds or None for default, applies to run but not test
      data: any data files available to the benchmark as runfiles
      tags: custom tags for the test
      timeout: the timeout for smoke testing the benchmark
      target_compatible_with: constraints for the test target
      redirect_stderr: if True, redirects stdout (seastar logging, mostly) to a file
                       so that it does not overwhelm the result output
      exec_in_shm: if True, the benchmark will execute in a subdir in /dev/shm which leads
                   to very fast IO, but the inability to resolve paths relative to the bazel
                   workspace root, so False is useful for benchmarks that need to access their
                   runfiles
    """

    # We require this naming convention as we do things like extract
    # the list of all benchmarks using a name-based query.
    if not name.endswith("_rpbench"):
        fail("benchmark names must end with _rpbench")

    args = [
        "--blocked-reactor-notify-ms 2000000",
        "--abort-on-seastar-bad-alloc",
    ] + args

    if _has_flags(args, "-m", "--memory"):
        fail("Use `memory=\"XGiB\"` test parameter instead of -m/--memory")
    if _has_flags(args, "-c", "--smp"):
        fail("Use `cpu=N` test parameter instead of -c/--smp")
    if _has_flags(args, "--runs"):
        fail("Use `runs=N` test parameter instead of --runs")
    if _has_flags(args, "--duration"):
        fail("Use `duration=N` test parameter instead of --duration")

    args.append("-m{}".format(memory))
    args.append("-c{}".format(cpu))
    resource_tags = [
        "resources:cpu:{}".format(cpu),
        # This is always defined in MiB for Bazel
        "resources:memory:{}".format(_parse_bytes(memory) / (1 << 20)),
    ]

    tags = tags + ["bench"]

    binary_name = name + "_binary"

    native.cc_binary(
        name = binary_name,
        srcs = srcs,
        defines = defines,
        deps = deps,
        testonly = True,
        copts = redpanda_copts(),
        features = [
            "layering_check",
        ],
        tags = tags,
        env = env,
        data = data,
    )

    # below here targets use a wrapper which takes the binary as the first
    # argument and which should be included in the runfiles data
    args = ["$(rootpath :{})".format(binary_name)] + args
    data = data + [":" + binary_name]

    env = env | {
        "MB_EXEC_IN_SHM": "1" if exec_in_shm else "0",
        "MB_REDIRECT_STDERR_DEFAULT": "1" if redirect_stderr else "0",
    }

    wrapper_src = ["//tools:bench_wrapper"]

    binary_args = []
    if runs != None:
        binary_args.append("--runs={}".format(runs))
    if duration != None:
        binary_args.append("--duration={}".format(duration))

    # to run a benchmark in the right way, we need to wrap it in bench-wrapper.sh,
    # which can cd to the right location and make other adjustments
    native.sh_binary(
        name = name,
        srcs = wrapper_src,
        args = args,
        data = data,
        env = env,
        testonly = True,
    )

    test_data, test_env = _test_options()
    native.sh_test(
        name = name + "_test",
        timeout = timeout,
        tags = resource_tags + tags,
        srcs = wrapper_src,
        env = env | test_env,
        args = args + ["--iterations=1 --runs=1 --duration=0 --no-stdout --overprovisioned"],
        data = data + test_data,
        target_compatible_with = target_compatible_with,
    )
