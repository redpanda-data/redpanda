import os
from typing import Any

from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.mark import ignore
from ducktape.tests.test import TestContext

from rptest.utils.type_utils import rcast
from rptest.util import FIPSMode, get_fips_mode


def allocate_and_free(cluster, logger):
    num_nodes = cluster.num_available_nodes()
    logger.debug(f"skip_debug_mode:: allocating {num_nodes} nodes")
    spec = ClusterSpec.simple_linux(num_nodes)
    nodes = cluster.alloc(spec)
    logger.debug(f"skip_debug_mode:: freeing up {num_nodes} nodes")
    cluster.free(nodes)


def cleanup_on_early_exit(caller: Any):
    """
    Cleans up on early exit to avoid errors due to unused resources.

    By default, the nodes we asked for are allocated and then freed,
    but if a method called `early_exit_hook` is defined on the class
    then it is called instead of the default action.
    """
    if hook := getattr(caller, "early_exit_hook", None):
        assert callable(hook), (
            f"{type(caller).__name__}.early_exit_hook should be a method which can be called to set up early exit from test"
        )
        hook()

    caller.logger.debug("Cleaning up unused nodes.")

    if test_context := getattr(caller, "test_context", None):
        test_context = rcast(TestContext, test_context)
        allocate_and_free(test_context.cluster, caller.logger)


def is_debug_mode():
    return os.environ.get("BUILD_TYPE", None) in ["debug", "sanitizer"]


def is_ubsan():
    """
    Returns True if redpanda is built with UBSAN enabled.
    """
    # For now, we just assume ubsan is only in debug mode
    return is_debug_mode()


def is_asan():
    """
    Returns True if redpanda is built with ASAN enabled.
    """
    # For now, we just assume asan is only in debug mode
    return is_debug_mode()


def skip_debug_mode(*args: Any, **kwargs: Any):
    """
    Test method decorator which signals to the test runner to ignore a given test.

    Example::

        When no parameters are provided to the @ignore decorator, ignore all parametrizations of the test function

        @skip_debug_mode  # Ignore all parametrizations
        @parametrize(x=1, y=0)
        @parametrize(x=2, y=3)
        def the_test(...):
            ...

    Example::

        If parameters are supplied to the @skip_debug_mode decorator, only skip the parametrization with matching parameter(s)

        @skip_debug_mode(x=2, y=3)
        @parametrize(x=1, y=0)  # This test will run as usual
        @parametrize(x=2, y=3)  # This test will be ignored
        def the_test(...):
            ...
    """
    if is_debug_mode():
        return ignore(*args, **kwargs)
    else:
        return args[0]


def ignore_if_not_debug(*args, **kwargs):
    """
    Test method decorator which ignores (skips) a test if redpanda is not debug mode.
    """
    if not is_debug_mode():
        return ignore(*args, **kwargs)
    else:
        return args[0]


def ignore_if_not_ubsan(*args, **kwargs):
    """
    Test method decorator which ignores (skips) a test if redpanda is not built
    with UBSAN enabled.
    """
    if not is_ubsan():
        return ignore(*args, **kwargs)
    else:
        return args[0]


def ignore_if_not_asan(*args, **kwargs):
    """
    Test method decorator which ignores (skips) a test if redpanda is not built
    with ASAN enabled.
    """
    if not is_asan():
        return ignore(*args, **kwargs)
    else:
        return args[0]


def in_fips_environment() -> bool:
    return get_fips_mode() != FIPSMode.disabled


def skip_fips_mode(*args: Any, **kwargs: Any):
    """
    Decorator indicating that the test should not run in FIPS mode.

    Ideally all tests should run in FIPS mode. The following are some situations
    in which skipping FIPS mode is required.

    * Exercising a known non-FIPS condition (e.g. virtual-host vs path style
    testing).

    * We can't test it in FIPS mode because of infrastructure issues, but the
    implementation doesn't change between FIPS and non-FIPS (auditing & OCSF
    server).

    * Certain license tests (since enabling FIPS mode enables enterprise license
    requirement).

    Example::

        When no parameters are provided to the @ignore decorator, ignore all parametrizations of the test function

        @skip_fips_mode  # Ignore all parametrizations
        @parametrize(x=1, y=0)
        @parametrize(x=2, y=3)
        def the_test(...):
            ...

    Example::

        If parameters are supplied to the @skip_fips_mode decorator, only skip the parametrization with matching parameter(s)

        @skip_fips_mode(x=2, y=3)
        @parametrize(x=1, y=0)  # This test will run as usual
        @parametrize(x=2, y=3)  # This test will be ignored
        def the_test(...):
            ...
    """
    if in_fips_environment():
        return ignore(*args, **kwargs)
    else:
        return args[0]
