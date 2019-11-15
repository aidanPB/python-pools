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
from asyncio import Condition as ACondition
from collections import deque
import collections.abc as collabc
import itertools
from typing import AbstractSet, AsyncGenerator, AsyncIterator
from typing import Generator, Optional, Set, TypeVar
try:
    from threading import Condition
except ImportError:
    from dummy_threading import Condition
import sys
if sys.version_info >= (3, 8):
    from typing import Protocol as Proto
else:
    from typing import Generic as Proto

__all__ = ("Pool",
           "PopulationPool",
           "AsyncPool",
           "AsyncPopulationPool",
           "SimpleIntPool",
           "SetBasedPopulationPool",
           "SetBasedAsyncPopPool",
           "AsyncIteratorPool",
           )

T = TypeVar('T')
OpT = Optional[T]
R_co = TypeVar('R_co', covariant=True)
H = TypeVar('H', bound=collabc.Hashable)

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
        self._stopped = False

    def send(self, value):
        """Request an int, or return one."""
        if self._stopped: raise StopIteration
        if not isinstance(value, int):
            if len(self._rvq) > 0:
                try:
                    return self._rvq.popleft()
                except IndexError:
                    yv, self._top = self._top, next(self._src)
                    return yv
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
            self._stopped = True
            raise StopIteration from exc

class SetBasedPopulationPool(collabc.Generator,
                             PopulationPool[H, None]):
    """A population pool backed by a set."""
    def __init__(self,
                 ivals: Optional[AbstractSet[H]] = None
                 ) -> None:
        if ivals is None:
            self._set = Set[H]()
        else:
            self._set = {ivals}
        self._con = Condition()
        self._stopped = False

    def populate(self, val: H, *args: H) -> None:
        #Can't populate a closed pool:
        if self._stopped: raise StopIteration
        if args is None or len(args) == 0:
            self._set.add(val)
            count = 1
        else:
            argset = {args}
            count = 1 + len(argset)
            self._set.add(val)
            self._set |= argset
        with self._con as c:
            c.notify(count)

    def send(self, value: Optional[H]) -> Optional[H]:
        """Request an object from the pool, or supply one."""
        if self._stopped: raise StopIteration
        if value is None:
            with self._con as c:
                while len(self._set) == 0:
                    c.wait()
                    if self._stopped: raise StopIteration
                return self._set.pop()
        else:
            if value not in self._set:
                self._set.add(value)
                with self._con as c:
                    c.notify()

    def throw(self, typ, val=None, tb=None):
        """Stop iterating and close the pool with an exception."""
        try:
            return super().throw(typ, val, tb)
        except (Exception, GeneratorExit) as exc:
            self._stopped = True
            with self._con as c:
                c.notify_all()
            raise StopIteration from exc

class SetBasedAsyncPopPool(collabc.AsyncGenerator,
                           AsyncPopulationPool[H]):
    """An asynchronous population pool backed by a set."""
    def __init__(self, ivals: Optional[AbstractSet[H]] = None):
        self._stopped = False
        self._set = Set[H]() if ivals is None else {ivals}
        self._ac = None
        self._stopped = False

    async def apopulate(self, val: H, *args: H) -> None:
        #Can't populate a closed pool:
        if self._stopped: raise StopAsyncIteration
        if self._ac is None: self._ac = ACondition()
        if args is None or len(args) == 0:
            self._set.add(val)
            count = 1
        else:
            argset = {args}
            argset.add(val)
            count = len(argset)
            self._set |= argset
        async with self._ac:
            self._ac.notify(count)

    async def asend(self, value: Optional[H]) -> Optional[H]:
        if self._stopped: raise StopAsyncIteration
        if self._ac is None: self._ac = ACondition()
        if value is None:
            async with self._ac:
                while len(self._set) == 0:
                    self._ac.wait()
                    if self._stopped: raise StopAsyncIteration
                return self._set.pop()
        else:
            if value not in self._set:
                self._set.add(value)
                await self._ac.acquire()
                self._ac.notify()
                self._ac.release()

    async def athrow(self, typ, val=None, tb=None) -> None:
        try:
            return await super().athrow(typ, val, tb)
        except (Exception, GeneratorExit) as exc:
            self._stopped = True
            if self._ac is not None:
                await self._ac.acquire()
                self._ac.notify_all()
                self._ac.release()
            raise StopAsyncIteration from exc

class AsyncIteratorPool(collabc.AsyncGenerator,
                         AsyncPool[T]):
    """An asynchronous pool that wraps another async iterator."""
    def __init__(self, base: AsyncIterator[T]):
        self._base = base
        self._basegen = isinstance(base, collabc.AsyncGenerator)
        self._yl = deque()
        self._rl = deque()
        self._ac = None
        self._stopped = False

    async def asend(self, value: OpT) -> OpT:
        if self._stopped: raise StopAsyncIteration
        if self._ac is None: self._ac = ACondition()
        if value is None:
            async with self._ac:
                if len(self._rl) == 0:
                    try:
                        if self._basegen:
                            yv = await self._base.asend(None)
                        else:
                            yv = await self._base.__anext__()
                        self._yl.append(yv)
                        return yv
                    except (Exception, GeneratorExit) as exc:
                        self._stopped = True
                        self._ac.notify_all()
                        raise StopAsyncIteration from exc
                else:
                    yv = self._rl.popleft()
                    self._yl.append(yv)
                    return yv
        else:
            async with self._ac:
                if value in self._yl:
                    self._yl.remove(value)
                    self._rl.append(value)

    async def athrow(self, typ, val=None, tb=None):
        try:
            if self._basegen:
                return await self._base.athrow(typ, val, tb)
            else:
                return await super().athrow(typ, val, tb)
        except (Exception, GeneratorExit) as exc:
            self._stopped = True
            if self._ac is not None:
                await self._ac.acquire()
                self._ac.notify_all()
                self._ac.release()
            raise StopAsyncIteration from exc
