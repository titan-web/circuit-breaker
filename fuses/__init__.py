"""
Circuit breaker pattern implementation

Example:
    >>> fuses_manage = FusesManager()
    >>> f = fuses_manage.get_fuses('push', 0, 1, [RuntimeError])
    >>> try:
    >>>     with circuit(f) as a:
    >>>         # remote call raise error
    >>>         raise RuntimeError("self runtime error!")
    >>>
    >>> except FusesOpenError as exp:
    >>>     print exp

"""

import functools
import contextlib

from .fuses import FusesManager, FusesOpenError, Fuses

__all__ = ("breaker", "circuit", "Fuses", "FusesOpenError", "FusesManager")
__version__ = "0.1.0"


def breaker(fuses):
    """
    decorator for circuit breaker pattern
    :param fuses: an instance of Fuses
    """

    def _wrapper(func):
        @functools.wraps(func)
        def __wrapper(*args, **kwargs):
            return fuses.handle(*args, **kwargs)

        return __wrapper

    return _wrapper


@contextlib.contextmanager
def circuit(fuses):
    """
    context for circuit breaker pattern
    :param fuses: an instance of Fuses
    """
    try:
        yield fuses.pre_handle()
    except Exception as exp:
        except_name = exp.__class__
        if except_name in fuses.exception_list():
            fuses.on_error()
        else:
            raise except_name(exp)
    else:
        fuses.on_success()
