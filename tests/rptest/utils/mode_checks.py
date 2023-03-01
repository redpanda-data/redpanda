import functools

from ducktape.cluster.cluster_spec import ClusterSpec


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


def skip_debug_mode(func):
    """
    Decorator applied to a test class method. The property `debug_mode` should be present
    on the object.

    If set to true, the wrapped function call is skipped, and a cleanup action
    is performed instead.

    If set to false, the wrapped function (usually a test case) is called.
    """
    @functools.wraps(func)
    def f(*args, **kwargs):
        assert args, 'skip_debug_mode must be placed on a test method in a class'

        caller = args[0]

        assert hasattr(
            caller, 'debug_mode'
        ), 'skip_debug_mode called on object which does not have debug_mode attribute'
        assert hasattr(
            caller,
            'logger'), 'skip_debug_mode called on object which has no logger'
        if caller.debug_mode:
            caller.logger.info(
                "Skipping test in debug mode (requires release build)")
            cleanup_on_early_exit(caller)
            return None
        return func(*args, **kwargs)

    return f


def skip_azure_blob_storage(func):
    """
    Decorator applied to a test class method. The property `azure_blob_storage` should be present
    on the object.

    If set to true, the wrapped function call is skipped, and a cleanup action
    is performed instead.

    If set to false, the wrapped function (usually a test case) is called.
    """
    @functools.wraps(func)
    def f(*args, **kwargs):
        assert args, 'skip_azure_blob_storage must be placed on a test method in a class'

        caller = args[0]

        assert hasattr(
            caller, 'azure_blob_storage'
        ), 'skip_azure_blob_storage called on object which does not have azure_blob_storage attribute'
        assert hasattr(
            caller, 'logger'
        ), 'skip_azure_blob_storage called on object which has no logger'
        if caller.azure_blob_storage:
            caller.logger.info(
                "Skipping Azure Blob Storage test in (requires S3)")
            cleanup_on_early_exit(caller)
            return None
        return func(*args, **kwargs)

    return f
