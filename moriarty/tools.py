from __future__ import annotations

import asyncio
import functools
import inspect
from functools import wraps

import anyio


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


def ensure_awaitable(func):
    if inspect.iscoroutinefunction(func):
        return func

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        nonlocal func
        if kwargs:
            func = functools.partial(func, **kwargs)
        return await anyio.to_thread.run_sync(func, *args)

    return wrapper
