"""
This module contains functions for building Redpanda tests. Prefer using the
methods in this module (e.g. redpanda_cc_gtest) over native Bazel functions
(e.g. cc_test) because it provides a centralized place for making behavior
changes. For example, redpanda_cc_gtest will automatically configure Seastar for
running tests, like setting a reasonable number of cores and amount of memory.
"""

load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")
load("@rules_python//python:defs.bzl", "py_binary", "py_test")
load(":internal.bzl", "antithesis_deps", "redpanda_copts")

def _reactor_args():
    """Returns additional reactor args for all reactor-using tests and benchmarks."""
    return select({
        "//bazel:io_uring_enabled": ["--reactor-backend=io_uring"],
        "//conditions:default": [],
    })

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
    """
    Returns common data dependencies, environment variables, and library deps for Redpanda tests.

    This function provides a centralized place to define common settings for all
    C++ tests, ensuring consistency and making it easier to manage test
    configurations.

    Returns:
        A tuple containing:
        - A list of common data dependencies needed by tests (e.g., suppression files).
        - A dictionary of common environment variables for test execution.
        - A list of common library dependencies for all tests.
    """
    data = [
        "//:ubsan_suppressions",
        "//:lsan_suppressions",
        "@current_llvm_toolchain//:llvm-symbolizer",
    ]
    env = {
        "BOOST_TEST_LOG_LEVEL": "test_suite",
        "BOOST_TEST_COLOR_OUTPUT": "0",
        "BOOST_TEST_CATCH_SYSTEM_ERRORS": "no",
        "BOOST_TEST_REPORT_LEVEL": "no",
        "BOOST_LOGGER": "HRF,test_suite",
        "ASAN_OPTIONS": "disable_coredump=0:abort_on_error=1",
        "ASAN_SYMBOLIZER_PATH": "$(rootpath @current_llvm_toolchain//:llvm-symbolizer)",
        "LSAN_OPTIONS": "suppressions=$(rootpath //:lsan_suppressions)",
        "UBSAN_OPTIONS": "symbolize=1:print_stacktrace=1:halt_on_error=1:abort_on_error=1:report_error_type=1:suppressions=$(rootpath //:ubsan_suppressions)",
        # see https://redpandadata.atlassian.net/wiki/x/BwDSUw
        "REDPANDA_RNG_SEEDING_MODE_DEFAULT": "fixed",
    }
    deps = antithesis_deps()
    return data, env, deps

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
        local_defines = [],
        flaky = False):
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
      flaky: whether the test is flaky (value passed to bazel attribute of the same name)
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

    args = args + _reactor_args()

    test_data, test_env, test_deps = _test_options()
    cc_test(
        name = name,
        timeout = timeout,
        srcs = srcs,
        defines = defines,
        deps = deps + test_deps,
        copts = redpanda_copts(),
        args = args,
        features = [
            "layering_check",
        ],
        tags = resource_tags + tags,
        env = {"RP_FIXTURE_ENV": "1"} | test_env | env,
        target_compatible_with = target_compatible_with,
        data = data + test_data,
        local_defines = local_defines,
        flaky = flaky,
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
    test_data, test_env, test_deps = _test_options()
    cc_test(
        name = name,
        timeout = timeout,
        srcs = srcs,
        defines = defines,
        deps = deps + test_deps,
        copts = redpanda_copts(),
        args = custom_args,
        features = [
            "layering_check",
        ],
        tags = [
            "fuzz",
        ],
        env = test_env | env,
        data = data + test_data,
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
        "--logger-log-level='exception=info'",
    ]

    # TODO(bazel): What are the right defaults here?
    _redpanda_cc_test(
        memory = memory or "1GiB",
        cpu = cpu or 2,
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
        target_compatible_with = [],
        data = [],
        tags = [],
        flaky = False):
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
        target_compatible_with = target_compatible_with,
        data = data,
        local_defines = ["IS_GTEST"],
        tags = tags,
        flaky = flaky,
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
        tags = [],
        flaky = False):
    deps.append(
        "//src/v/test_utils:boost_test_hooks",
    )

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
        local_defines = ["IS_BTEST"],
        tags = tags,
        flaky = flaky,
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
    test_data, test_env, test_deps = _test_options()
    cc_test(
        name = name,
        timeout = timeout,
        tags = [
            "resources:cpu:{}".format(cpu),
            "resources:memory:{}".format(_parse_bytes(memory) / (2 << 20)),
        ],
        srcs = srcs,
        defines = defines,
        copts = redpanda_copts(),
        local_defines = ["IS_BTEST"],
        deps = [
            "//src/v/test_utils:boost_result_redirect",
            "//src/v/test_utils:boost_test_hooks",
            "@boost//:test.so",
        ] + deps + test_deps,
        data = test_data,
        env = test_env,
    )

def redpanda_test_cc_library(
        name,
        srcs = [],
        hdrs = [],
        defines = [],
        local_defines = [],
        visibility = None,
        implementation_deps = [],
        deps = [],
        alwayslink = False):
    cc_library(
        name = name,
        srcs = srcs,
        hdrs = hdrs,
        defines = defines,
        local_defines = local_defines,
        visibility = visibility,
        include_prefix = native.package_name().removeprefix("src/v/"),
        implementation_deps = implementation_deps,
        deps = deps,
        copts = redpanda_copts(),
        testonly = True,
        features = [
            "layering_check",
        ],
        alwayslink = alwayslink,
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
        redirect_stderr = False,
        test_regex = None):
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
      runs: number of runs or None for default (applies to run but not test)
      duration: duration of a single run in seconds or None for default (applies to run but not test)
      data: any data files available to the benchmark as runfiles
      tags: custom tags for the test
      timeout: the timeout for smoke testing the benchmark
      redirect_stderr: if True, redirects stderr (seastar logging, mostly) to a file
                       so that it does not overwhelm the result output
      test_regex: optional regex passed as `-t <regex>` to the smoke test to select
                  a subset of benchmarks to run
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

    # all benches must include the hooks
    deps.append(
        "//src/v/test_utils:rpbench_hooks",
    )

    env = {
        # see https://redpandadata.atlassian.net/wiki/x/BwDSUw
        "REDPANDA_RNG_SEEDING_MODE_DEFAULT": "fixed",
    } | env

    tags = tags + ["bench"]

    test_data, test_env, test_deps = _test_options()

    binary_name = name + "_binary"
    cc_binary(
        name = binary_name,
        srcs = srcs,
        defines = defines,
        deps = deps + test_deps,
        testonly = True,
        copts = redpanda_copts(),
        features = [
            "layering_check",
        ],
        tags = tags,
        env = env,
        data = data,
    )

    args = ["$(rootpath :{})".format(binary_name)] + args + _reactor_args()
    env = env | {
        "MB_EXEC_IN_SHM": "1",
        "MB_REDIRECT_STDERR_DEFAULT": "1" if redirect_stderr else "0",
    }

    binary_args = []
    if runs != None:
        binary_args.append("--runs={}".format(runs))
    if duration != None:
        binary_args.append("--duration={}".format(duration))

    # to run a benchmark in the right way, we need to wrap it in bench-wrapper.sh,
    # which can cd to the right location and make other adjustments
    py_binary(
        name = name,
        srcs = ["//bazel:bench_wrapper"],
        main = "bench_wrapper.py",
        args = args + binary_args,
        data = data + [":" + binary_name],
        env = env,
        testonly = True,
    )

    # we write a wrapper to test the benchmark, which tries to
    # run it as quickly as possible in order to smoke test it
    test_args = args + ["--iterations=1 --runs=1 --duration=0 --no-stdout --overprovisioned"]
    if test_regex != None:
        test_args = test_args + ["-t {}".format(test_regex)]
    py_test(
        name = name + "_test",
        timeout = timeout,
        main = "bench_wrapper.py",
        tags = resource_tags + tags,
        srcs = ["//bazel:bench_wrapper"],
        env = test_env | env,
        args = test_args,
        data = [":" + binary_name] + data + test_data,
    )
