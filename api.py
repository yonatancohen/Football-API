import json
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette import status

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from db import FootballDBHandler
from utils import calculate_all_distances_fixed

# todo: support debug?
app = FastAPI(debug=True)

# configure CORS so Angular (localhost:4200) can talk
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_methods=["*"],
    allow_headers=["*"],
)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


class CreateGameRequest(BaseModel):
    player_id: int
    activate_at: str
    leagues: List[int]
    hint: Optional[str] = None


class GameRequest(BaseModel):
    game_id: int
    player_id: int


@app.post("/api/game")
async def create_game(request: CreateGameRequest):
    try:
        if request.player_id:
            # Calculate player
            results = calculate_all_distances_fixed(request.player_id, request.leagues)

            db_handler = FootballDBHandler()
            db_handler.create_game(activate_at=request.activate_at, distance=results, hint=request.hint, leagues=request.leagues)

            return Response(status_code=status.HTTP_200_OK)

        return Response(status_code=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.get("/api/game/")
async def get_game(game_id: Optional[int] = None):
    try:
        # todo: get game from db/cache
        db_handler = FootballDBHandler()
        game = db_handler.get_game(game_id)
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
            # todo: get game from db/cache
            db_handler = FootballDBHandler()
            rank = db_handler.get_player_rank(body.game_id, body.player_id)
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


@app.get("/api/admin/leagues")
async def get_leagues():
    try:
        return FootballDBHandler().get_leagues()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.get("/api/admin/countries")
async def get_countries():
    try:
        return FootballDBHandler().get_countries()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.get("/api/admin/players")
async def search_players(query: str):
    try:
        return FootballDBHandler().get_autocomplete_players(player_name=query)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.get("/api/admin/players/{player_id}")
async def search_players(player_id: int):
    try:
        return FootballDBHandler().get_player(player_id)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
