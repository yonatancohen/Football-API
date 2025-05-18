from datetime import datetime, timedelta
from typing import Any, Optional


class GameCacheService:
    def __init__(self, db_handler):
        self.db = db_handler
        self.by_id_cache: dict[int, dict[str, Any]] = {}
        self.latest_cache: dict[str, Any] = {"expires": datetime.min, "data": None}
        self.rank_cache: dict[tuple[int, int], dict[str, Any]] = {}

    def _next_interval(self, now: datetime) -> datetime:
        minutes_to_next = 5 - (now.minute % 5)
        if minutes_to_next == 0:
            minutes_to_next = 5
        return now + timedelta(minutes=minutes_to_next, seconds=-now.second, microseconds=-now.microsecond)

    def get_game_by_row_number(self, game_number: Optional[int] = None):
        now = datetime.now()

        if game_number is not None:
            entry = self.by_id_cache.get(game_number)
            if entry and now < entry["expires"]:
                print('CACHED ANSWER! (game)')

                return entry["data"]
            result = self.db.get_customer_game(game_number)
            if result:
                self.by_id_cache[game_number] = {
                    "data": result,
                    "expires": now + timedelta(days=1)
                }
                print('NOT CACHE (game)')
                return result

        if now < self.latest_cache["expires"] and self.latest_cache["data"] is not None:
            print('CACHED ANSWER!')
            return self.latest_cache["data"]

        result = self.db.get_customer_game(None)
        self.latest_cache["data"] = result
        self.latest_cache["expires"] = self._next_interval(now)
        print('NOT CACHE')
        return result

    def revoke_game(self, game_id: int):
        self.by_id_cache.pop(game_id, None)

    def clear_game_cache(self):
        self.by_id_cache.clear()
        self.latest_cache = {"expires": datetime.min, "data": None}

    def get_rank(self, game_number: int, player_id: int):
        now = datetime.now()
        key = (game_number, player_id)
        entry = self.rank_cache.get(key)

        if entry and now < entry["expires"]:
            return entry["data"]

        rank = self.db.get_player_rank(game_number, player_id)
        self.rank_cache[key] = {
            "data": rank,
            "expires": now + timedelta(hours=1)
        }

        return rank

    def revoke_rank(self, game_id: int, player_id: int):
        self.rank_cache.pop((game_id, player_id), None)

    def revoke_ranks_for_game(self, game_id: int):
        keys_to_remove = [key for key in self.rank_cache if key[0] == game_id]
        for key in keys_to_remove:
            self.rank_cache.pop(key, None)

        self.revoke_latest_game(game_id)

    def revoke_latest_game(self, game_id: int):
        cached = self.latest_cache.get("data")
        if cached and isinstance(cached, dict) and cached.get("id") == game_id:
            self.latest_cache = {"expires": datetime.min, "data": None}

    def clear_rank_cache(self):
        self.rank_cache.clear()
