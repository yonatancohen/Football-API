from typing import List, Optional

from pydantic import BaseModel


class CreateGameRequest(BaseModel):
    player_id: int
    activate_at: str
    leagues: List[int]
    hint: Optional[str] = None


class GameRequest(BaseModel):
    game_id: int
    player_id: int


class PlayerUpdateRequest(BaseModel):
    first_name_he: str
    last_name_he: str
    display_name_he: str
    nationality_id: int


class LoginRequest(BaseModel):
    username: str
    password: str
