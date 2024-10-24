from typing import TypeVar

T = TypeVar('T')


def rcast(clazz: type[T], obj: object) -> T:
    """
    Similar to cast(clazz, obj) except that it also checks that obj has the
    the expected type at runtime, to avoid a silent hole in typing if the
    object is of an unexpected type.
    """
    assert isinstance(
        obj,
        clazz), f"{obj.__class__.__name__} cannot be cast to {clazz.__name__}"
    return obj
