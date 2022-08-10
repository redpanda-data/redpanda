import functools


def skip_debug_mode(func):
    @functools.wraps(func)
    def f(*args, **kwargs):
        assert args, 'skip_debug_mode must be placed on a test class method'

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
            return None
        return func(*args, **kwargs)

    return f
