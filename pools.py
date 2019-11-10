"""pools -- Generators with reuse

This module defines a simplistic "Pool" protocol as a
narrowing subtype of the generator protocol, and provides
some implementations.

Pools are generator-iterators that additionally provide the
following guarantees about their behaviour:

* They always yield a value of the *pooled type*.
* Every value they yield is distinct and unique...
* Unless a previously-yielded value is *returned* to the
  pool with the ``send()`` method.
* Values sent to a pool are ignored if they aren't of the
  pooled type.
* A pool should yield ``None`` when a value of the pooled
  type is sent to it.

What a pool should do with a value it never yielded, but is
of the pooled type, depends on the intended use-case of the
specific class.

Population pools extend this protocol with a ``populate()``
method, used to submit values to the pool without starting
or resuming it.

There are also asynchronous pools and population pools.
"""
import abc
from collections import deque
import collections.abc as collabc
import itertools
from typing import AsyncGenerator, Generator, Optional
from typing import TypeVar
import sys
if sys.version_info >= (3, 8):
    from typing import Protocol as Proto
else:
    from typing import Generic as Proto

__all__ = ("Pool", "PopulationPool", "AsyncPool",
           "AsyncPopulationPool", "SimpleIntPool",)

T = TypeVar('T')
OpT = Optional[T]
R_co = TypeVar('R_co', covariant=True)

class Pool(Generator[OpT, OpT, R_co], Proto[T, R_co]):
    """A pool of objects that can be returned for reuse."""
    pass

class AsyncPool(AsyncGenerator[OpT, OpT], Proto[T]):
    """An asynchronous pool of objects that can be returned
    for reuse."""
    pass

class PopulationPool(Pool[T, R_co], Proto[T, R_co]):
    """A pool of objects that can be populated before or after
    being started."""
    @abc.abstractmethod
    def populate(self, val: T, *args: T) -> None:
        """Populates this pool with one or more values."""
        pass

class AsyncPopulationPool(AsyncPool[T], Proto[T]):
    """An asynchronous pool that can be populated before or
    after being started."""
    @abc.abstractmethod
    async def apopulate(self, val: T, *args: T
                        ) -> None:
        """Asynchronously populates this pool with one or
        more values."""
        pass

class SimpleIntPool(collabc.Generator, Pool[int, None]):
    """A simple source of integers."""
    def __init__(self):
        self._src = itertools.count()
        #The lowest int that has never been yielded:
        self._top = next(self._src)
        #The queue of returned values:
        self._rvq = deque()

    def send(self, value):
        """Request an int, or return one."""
        if not isinstance(value, int):
            if len(self._rvq) > 0:
                try:
                    return self._rvq.popleft()
                except IndexError:
                    yv, self._top = self._top, next(self._src)
                    return yv
                except GeneratorExit as ge:
                    raise StopIteration from ge
            yv, self._top = self._top, next(self._src)
            return yv
        else:
            if 0 <= value < self._top and value not in self._rvq:
                self._rvq.push(value)
            #Otherwise, silently swallow it.

    def throw(self, typ, val=None, tb=None):
        """Stop iterating and close the pool with an exception."""
        try:
            return super().throw(typ, val, tb)
        except (Exception, GeneratorExit) as exc:
            raise StopIteration from exc
