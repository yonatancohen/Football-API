from common import USERNAME, PASSWORD, JWT_EXPIRE_MINUTES, JWT_ALGORITHM, JWT_SECRET
from game.cache import GameCacheService
from game.db import FootballDBHandler
from game.auth import JWTAuth


game_service = GameCacheService(FootballDBHandler())
auth = JWTAuth(secret_key=JWT_SECRET, algorithm=JWT_ALGORITHM, expires_minutes=JWT_EXPIRE_MINUTES,
               username=USERNAME, password=PASSWORD)
