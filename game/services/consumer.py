import json
from typing import Optional

from fastapi import APIRouter, Request, Response, HTTPException
from starlette import status

from game.config import game_service
from game.db import FootballDBHandler
from game.limiter import limiter
from game.services.models import GameRequest

router = APIRouter(prefix="/api")


@router.get("/game")
async def get_customer_game(game_number: Optional[int] = None):
    try:
        game = game_service.get_game_by_row_number(game_number)
        if game:
            return {
                # "id": game["id"],
                "max_rank": game["max_rank"],
                "hint": game["hint"],
                "players": json.loads(game["players"]),
                "game_number": game["game_number"],
                "max_game_number": game["max_game_number"]
            }

        return Response(status_code=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/check-rank")
@limiter.limit("5/second")
async def check_response(request: Request, body: GameRequest):
    try:
        if body.game_number and body.player_id:
            rank = game_service.get_rank(body.game_number, body.player_id)
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


@router.get("/next-game")
async def get_next_game_start_date(response: Response):
    return Response(content=FootballDBHandler().get_countdown(),
                    headers={"Cache-Control": "public, max-age=30"})
