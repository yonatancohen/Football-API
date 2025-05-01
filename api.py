from fastapi.security import OAuth2PasswordRequestForm

import json
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Response, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette import status

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from auth import JWTAuth
from db import FootballDBHandler
from game.cache import GameCacheService
from utils import calculate_all_distances_fixed, parse_datetime

# todo: support debug?
app = FastAPI(debug=True)

# configure CORS so Angular (localhost:4200) can talk
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

auth = JWTAuth(
    secret_key="your-secret-key",
    algorithm="HS256",
    expires_minutes=30,
    username="a",
    password="a"
)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

game_service = GameCacheService(FootballDBHandler())


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


@app.get("/api/game/")
async def get_customer_game(game_id: Optional[int] = None):
    try:
        game = game_service.get_game(game_id)
        if game:
            return {
                "id": game["id"],
                "max_rank": game["max_rank"],
                "hint": game["hint"],
                "players": json.loads(game["players"]),
            }

        return Response(status_code=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.post("/api/check-rank")
@limiter.limit("5/second")
async def check_response(request: Request, body: GameRequest):
    try:
        if body.game_id and body.player_id:
            rank = game_service.get_rank(body.game_id, body.player_id)
            if rank:
                return rank

        return Response(status_code=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.get("/api/players")
async def get_players_by_leagues(leagues_id: Optional[str]):
    try:
        leagues_id = leagues_id.split(',')
        return FootballDBHandler().get_autocomplete_players(leagues_id)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


### Admin URLS ###
@app.post("/api/admin/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if not auth.authenticate_user(form_data.username, form_data.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )
    access_token = auth.create_access_token({"sub": form_data.username})
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/api/admin/leagues")
async def get_leagues(user: str = Depends(auth)):
    try:
        return FootballDBHandler().get_leagues()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.get("/api/admin/countries")
async def get_countries(user: str = Depends(auth)):
    try:
        return FootballDBHandler().get_countries()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.get("/api/admin/players")
async def search_players(query: str, user: str = Depends(auth)):
    try:
        return FootballDBHandler().get_autocomplete_players(player_name=query)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.get("/api/admin/players/{player_id}")
async def get_player(player_id: int, user: str = Depends(auth)):
    try:
        return FootballDBHandler().get_player(player_id)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.post("/api/admin/players/{player_id}")
async def get_player(player_id: int, request: PlayerUpdateRequest, user: str = Depends(auth)):
    try:
        return FootballDBHandler().update_player(player_id, request.first_name_he, request.last_name_he, request.display_name_he,
                                                 request.nationality_id)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.get("/api/admin/games/search")
async def search_game(game_date: Optional[str] = None, player_name: Optional[str] = None, user: str = Depends(auth)):
    try:
        db_handler = FootballDBHandler()
        return db_handler.search_game(game_date, player_name)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.post("/api/admin/games")
async def create_game(request: CreateGameRequest, user: str = Depends(auth)):
    try:
        if request.player_id:
            # Calculate player
            results = calculate_all_distances_fixed(request.player_id, request.leagues)

            db_handler = FootballDBHandler()
            db_handler.create_game(activate_at=parse_datetime(request.activate_at), distance=results, hint=request.hint,
                                   leagues=request.leagues)

            return Response(status_code=status.HTTP_200_OK)

        return Response(status_code=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.get("/api/admin/games/{game_id}")
async def get_game(game_id: int, user: str = Depends(auth)):
    try:
        db_handler = FootballDBHandler()
        result = db_handler.get_game(game_id)
        if result:
            return result

        return Response(status_code=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.put("/api/admin/games/{game_id}")
async def update_admin_game(game_id: int, request: CreateGameRequest, user: str = Depends(auth)):
    try:
        results = calculate_all_distances_fixed(request.player_id, request.leagues)

        db_handler = FootballDBHandler()
        db_handler.update_game(game_id=game_id, activate_at=parse_datetime(request.activate_at), distance=results, hint=request.hint,
                               leagues=request.leagues)

        game_service.revoke_game(game_id=game_id)
        game_service.revoke_ranks_for_game(game_id=game_id)

        return Response(status_code=status.HTTP_200_OK)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
