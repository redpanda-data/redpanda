import os

from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.mark import ignore


def allocate_and_free(cluster, logger):
    num_nodes = cluster.num_available_nodes()
    logger.debug(f'skip_debug_mode:: allocating {num_nodes} nodes')
    spec = ClusterSpec.simple_linux(num_nodes)
    nodes = cluster.alloc(spec)
    logger.debug(f'skip_debug_mode:: freeing up {num_nodes} nodes')
    cluster.free(nodes)


def cleanup_on_early_exit(caller):
    """
    Cleans up on early exit to avoid errors due to unused resources.

    By default, the nodes we asked for are allocated and then freed,
    but if a method called `early_exit_hook` is defined on the class
    then it is called instead of the default action.
    """
    name = type(caller).__name__
    if hook := getattr(caller, 'early_exit_hook', None):
        assert callable(
            hook
        ), f'{name.early_exit_hook} should be a method which can be called to set up early exit from test'
        hook()

    caller.logger.debug(f'Cleaning up unused nodes.')

    if test_context := getattr(caller, 'test_context', None):
        allocate_and_free(test_context.cluster, caller.logger)


def is_debug_mode():
    return os.environ.get('BUILD_TYPE', None) == 'debug'


def skip_debug_mode(*args, **kwargs):
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
        return ignore(args, kwargs)
    else:
        return args[0]


def in_fips_environment() -> bool:
    """
    Returns True if the file /proc/sys/crypto/fips_enabled is present and
    contains '1', otherwise returns False.
    """
    fips_file = "/proc/sys/crypto/fips_enabled"
    if os.path.exists(fips_file) and os.path.isfile(fips_file):
        with open(fips_file, 'r') as f:
            contents = f.read().strip()
            return contents == '1'

    return False


def skip_fips_mode(*args, **kwargs):
    """
    Test method decorator which signals to the test runner to ignore a given test.

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
        return ignore(args, kwargs)
    else:
        return args[0]
