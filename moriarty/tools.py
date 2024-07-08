from __future__ import annotations

import asyncio
import functools
import inspect
import random
from functools import wraps

import anyio
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


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


def sample_as_weights(available_priorities: list[int]) -> int:
    total = sum(available_priorities)
    weights = [p / total for p in available_priorities]
    chosen_priority = random.choices(available_priorities, weights)[0]
    return chosen_priority


class FlexibleModel(BaseModel):
    """
    This allow us provide camel and snake case in the same time
    """

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        from_attributes=True,
    )
