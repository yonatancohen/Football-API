from functools import wraps
from datetime import datetime, timedelta
from typing import Any, Optional, Callable, Awaitable
from fastapi import Request


def game_cache():
    """
    Caches game lookups:
    - If game_id is provided: cache result by game_id for 1 day.
    - If game_id is None: cache the latest game until the next 5-minute boundary.
    Designed to wrap FastAPI endpoint functions.
    """

    def decorator(func: Callable[..., Awaitable[Any]]):
        by_id_cache: dict[int, dict[str, Any]] = {}
        latest_cache: dict[str, Any] = {"expires": datetime.min, "data": None}

        def next_interval(now: datetime) -> datetime:
            minutes_to_next = 5 - (now.minute % 5)
            if minutes_to_next == 0:
                minutes_to_next = 5
            return now + timedelta(minutes=minutes_to_next, seconds=-now.second, microseconds=-now.microsecond)

        @wraps(func)
        async def wrapper(*args, **kwargs):
            game_id: Optional[int] = kwargs.get("game_id")
            now = datetime.now()

            if game_id is not None:
                entry = by_id_cache.get(game_id)
                if entry and now < entry["expires"]:
                    return entry["data"]
                result = await func(*args, **kwargs)
                by_id_cache[game_id] = {
                    "data": result,
                    "expires": now + timedelta(days=1)
                }
                return result

            if now < latest_cache["expires"] and latest_cache["data"] is not None:
                return latest_cache["data"]

            result = await func(*args, **kwargs)
            latest_cache["data"] = result
            latest_cache["expires"] = next_interval(now)
            return result

        wrapper.cache_clear = lambda: (by_id_cache.clear(), latest_cache.update({"expires": datetime.min, "data": None}))
        return wrapper

    return decorator


def rank_cache():
    """
    Caches player rank responses for 1 hour based on (game_id, player_id).
    Designed to wrap FastAPI endpoint functions.
    """

    def decorator(func: Callable[..., Any]):
        cache: dict[tuple[int, int], dict[str, Any]] = {}

        @wraps(func)
        async def wrapper(request: Request, body, *args, **kwargs):
            game_id = body.game_id
            player_id = body.player_id
            now = datetime.now()

            if game_id and player_id:
                key = (game_id, player_id)
                entry = cache.get(key)
                if entry and now < entry["expires"]:
                    return entry["data"]

                result = await func(request, body, *args, **kwargs)
                cache[key] = {
                    "data": result,
                    "expires": now + timedelta(hours=1)
                }
                return result

            return await func(request, body, *args, **kwargs)

        wrapper.cache_clear = lambda: cache.clear()
        return wrapper

    return decorator
