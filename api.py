from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette import status

from db import FootballDBHandler
from utils import calculate_all_distances_fixed, parse_datetime

# todo: support debug
app = FastAPI(debug=True)

# configure CORS so Angular (localhost:4200) can talk
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class CreateGameRequest(BaseModel):
    player_id: int
    activate_at: str


@app.post("/api/game")
async def create_game(request: CreateGameRequest):
    try:
        if request.player_id:
            # Calculate player
            results = calculate_all_distances_fixed(request.player_id)

            db_handler = FootballDBHandler()
            db_handler.create_game(request.activate_at, results)

            return Response(status_code=status.HTTP_200_OK)

        return Response(status_code=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.get("/api/game")
async def get_game(game_id: Optional[int] = None):
    try:
        # todo: get game from db/cache
        db_handler = FootballDBHandler()
        game = db_handler.get_game(game_id)
        if game:
            return {
                "id": game["id"],
                "max_rank": game["max_rank"]
            }

        return Response(status_code=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


class GameRequest(BaseModel):
    game_id: int
    player_id: int


@app.post("/api/check-rank")
async def check_response(request: GameRequest):
    try:
        if request.game_id and request.player_id:
            # todo: get game from db/cache
            db_handler = FootballDBHandler()
            rank = db_handler.get_player_rank(request.game_id, request.player_id)
            if rank:
                return rank

        return Response(status_code=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
