import json
from typing import Optional

from fastapi import APIRouter, Request, Response, HTTPException
from starlette import status

from game.config import game_service
from game.db import FootballDBHandler
from game.limiter import limiter
from game.services.models import GameRequest

router = APIRouter(prefix="/api")


@router.get("/game/")
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


@router.post("/check-rank")
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


@router.get("/players")
async def get_players_by_leagues(leagues_id: Optional[str]):
    try:
        leagues_id = leagues_id.split(',')
        return FootballDBHandler().get_autocomplete_players(leagues_id)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
