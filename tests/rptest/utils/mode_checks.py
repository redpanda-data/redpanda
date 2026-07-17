import inspect
import os
import sys
import warnings
from collections.abc import Callable
from typing import Any, TypeVar, overload

from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.mark import ignore
from ducktape.tests.test import Test, TestContext

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


def in_cdt() -> bool:
    """
    True when running against real cloud infrastructure (a CDT run).

    CDT exports CLOUD_PROVIDER=aws|gcp|azure before invoking ducktape; local and
    dockerized-CI runs leave it unset (treated as "docker"). Read the env var
    directly here, mirroring is_debug_mode()'s BUILD_TYPE check, to avoid an
    import cycle: rptest.services.redpanda already imports this module.
    """
    return os.environ.get("CLOUD_PROVIDER", "docker") != "docker"


_T = TypeVar("_T")


def _skip_method_in_cdt(method: object, reason: str | None) -> None:
    if reason is not None:
        setattr(method, "_skip_in_cdt_reason", reason)
    if in_cdt():
        # ignore() attaches IgnoreAll in place (ducktape marks mutate fun.__dict__),
        # so the mark lands on `method` itself; no re-bind of the return is needed.
        ignore(method)


def _mark_own_cdt_tests(cls: type) -> None:
    # Mark only DIRECTLY-DEFINED test methods (a method ducktape treats as a test
    # carries `marks` from @cluster/@matrix/@parametrize). ignore() mutates the
    # function's __dict__ in place, so the mark is visible on the class.
    for member in list(vars(cls).values()):
        if callable(member) and hasattr(member, "marks"):
            ignore(member)


def _inherited_cdt_test_methods(cls: type) -> list[str]:
    # Test methods carrying ducktape marks that are inherited from a base and not
    # overridden on cls (i.e. absent from cls's own __dict__).
    own = vars(cls)
    return sorted(
        {
            name
            for base in cls.__mro__[1:]
            for name, member in vars(base).items()
            if callable(member) and hasattr(member, "marks") and name not in own
        }
    )


def _cross_module_cdt_test_methods(cls: type) -> list[str]:
    # Marked test methods inherited from a base defined in ANOTHER module. Since
    # skip_file_in_cdt only marks methods defined in this module, these would not
    # be covered by a module-scope opt-out here.
    own = vars(cls)
    return sorted(
        {
            name
            for base in cls.__mro__[1:]
            if getattr(base, "__module__", None) != cls.__module__
            for name, member in vars(base).items()
            if callable(member) and hasattr(member, "marks") and name not in own
        }
    )


def _skip_class_in_cdt(cls: type, reason: str | None) -> None:
    if reason is not None:
        setattr(cls, "_skip_in_cdt_reason", reason)
    # ducktape marks are function-scoped: marking an inherited (shared) base method
    # would skip it for EVERY subclass, so class-scope can't safely cover inherited
    # tests. Fail loudly rather than silently leave an inherited test running in CDT.
    inherited = _inherited_cdt_test_methods(cls)
    if inherited:
        raise RuntimeError(
            f"@skip_in_cdt on {cls.__name__}: test method(s) {inherited} are "
            f"inherited; class-scope skip only covers directly-defined methods. "
            f"Apply @skip_in_cdt to the method(s), or to the class that defines them."
        )
    if in_cdt():
        _mark_own_cdt_tests(cls)


@overload
def skip_in_cdt(target: _T, /) -> _T: ...


@overload
def skip_in_cdt(*, reason: str | None = ...) -> Callable[[_T], _T]: ...


def skip_in_cdt(*args: Any, reason: str | None = None) -> Any:
    """
    Opt a test method or test class out of CDT (real-cloud) runs.

    No-op locally and in dockerized CI (CLOUD_PROVIDER unset/"docker"); there the
    test runs exactly as today. In CDT it attaches ducktape's ignore mark, so the
    test is collected and reported as IGNORE (visible, not silently dropped) and
    no cluster is allocated for it.

    Method scope::

        @skip_in_cdt(reason="HTTP-API only; no cloud-infra signal")
        @cluster(num_nodes=3)
        def test_x(self): ...

    Class scope marks the class's DIRECTLY-DEFINED test methods; it raises if the
    class's tests are inherited from a base (apply to the method or the base
    instead), since ducktape marks are function-scoped and can't be class-scoped
    safely::

        @skip_in_cdt(reason="pure HTTP surface")
        class MyTest(RedpandaTest): ...

    `reason` is optional source-level documentation; it is stored on the target
    as `_skip_in_cdt_reason` and is NOT forwarded to ducktape's ignore (which
    would misread it as a parametrization matcher and silently un-skip the test).
    """

    def decorate(target: _T) -> _T:
        if inspect.isclass(target):
            _skip_class_in_cdt(target, reason)
        else:
            _skip_method_in_cdt(target, reason)
        return target

    if args:
        # bare usage: @skip_in_cdt
        return decorate(args[0])
    # called usage: @skip_in_cdt(reason=...)
    return decorate


def skip_file_in_cdt(
    reason: str | None = None, *, namespace: dict[str, Any] | None = None
):
    """
    Opt every test class DEFINED IN THE CALLING MODULE out of CDT runs.

    Call at module scope, AFTER the class definitions (typically the last line of
    the file); if called earlier the classes are not yet in the module globals
    and nothing is marked. Attaches no ignore marks locally or in dockerized CI
    (tests run as today); the misuse warning below fires regardless of
    environment, so a misapplied opt-out is caught at authoring time.

    Marks the test methods DEFINED IN this module's classes. Only classes whose
    __module__ matches are touched (an imported base class is left alone), and only
    their directly-defined methods are marked -- a same-module base's methods are
    marked too, so its subclasses are covered via the shared function object.
    Because that mark lives on the function object itself, a subclass of the same
    base in ANOTHER module would also be skipped in CDT (safe as long as opted-out
    bases share no test methods with non-opted-out suites). A class here that
    inherits test methods from another module -- whether or not it also defines its
    own -- leaves those inherited tests uncovered; unlike the class decorator this
    warns rather than raising, since module-scope means "the tests defined in this
    file". `namespace` defaults to the caller's module globals; pass an explicit
    dict in unit tests.
    """
    if namespace is None:
        namespace = sys._getframe(1).f_globals
    module_name = namespace.get("__name__")
    for value in list(namespace.values()):
        if (
            inspect.isclass(value)
            and issubclass(value, Test)
            and value.__module__ == module_name
        ):
            if reason is not None:
                setattr(value, "_skip_in_cdt_reason", reason)
            if in_cdt():
                _mark_own_cdt_tests(value)
            # Test methods inherited from another module aren't covered here
            # (skip_file marks module-local methods; the class decorator would
            # raise). Warn for both inherited-only and mixed classes, so a
            # misapplied opt-out is visible rather than a silent CDT no-op.
            cross = _cross_module_cdt_test_methods(value)
            if cross:
                warnings.warn(
                    f"skip_file_in_cdt: {value.__name__} in {module_name} inherits "
                    f"test method(s) {cross} from another module; they will NOT be "
                    f"skipped in CDT -- opt them out in the defining module or with a "
                    f"method-scope @skip_in_cdt.",
                    stacklevel=2,
                )
