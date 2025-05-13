from game.cache import GameCacheService
from game.db import FootballDBHandler
from game.auth import JWTAuth

game_service = GameCacheService(FootballDBHandler())
auth = JWTAuth(secret_key="sport5-jwt-sECrEt!", algorithm="HS256", expires_minutes=60,
               username="sport5admin", password="Sport5Admin2025!")
