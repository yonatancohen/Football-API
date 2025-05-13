from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.security import OAuth2PasswordRequestForm
from starlette import status

from game.config import auth, game_service
from game.db import FootballDBHandler
from game.services.models import PlayerUpdateRequest, CreateGameRequest
from utils import calculate_all_distances_fixed, parse_datetime

router = APIRouter(prefix="/api/admin")


### Admin URLS ###
@router.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if not auth.authenticate_user(form_data.username, form_data.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )
    access_token = auth.create_access_token({"sub": form_data.username})
    return {"access_token": access_token, "token_type": "bearer"}


@router.get("/leagues")
async def get_leagues(user: str = Depends(auth)):
    try:
        return FootballDBHandler().get_leagues()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/countries")
async def get_countries(user: str = Depends(auth)):
    try:
        return FootballDBHandler().get_countries()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/players")
async def search_players(query: str, user: str = Depends(auth)):
    try:
        return FootballDBHandler().get_autocomplete_players(player_name=query)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/players/{player_id}")
async def get_player(player_id: int, user: str = Depends(auth)):
    try:
        return FootballDBHandler().get_player(player_id)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/players/{player_id}")
async def get_player(player_id: int, request: PlayerUpdateRequest, user: str = Depends(auth)):
    try:
        return FootballDBHandler().update_player(player_id, request.first_name_he, request.last_name_he, request.display_name_he,
                                                 request.nationality_id)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/games/search")
async def search_game(game_date: Optional[str] = None, player_name: Optional[str] = None, game_number: Optional[str] = None,
                      user: str = Depends(auth)):
    try:
        db_handler = FootballDBHandler()
        return db_handler.search_game(game_date, player_name, game_number)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/games")
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


@router.get("/games/{game_id}")
async def get_game(game_id: int, user: str = Depends(auth)):
    try:
        db_handler = FootballDBHandler()
        result = db_handler.get_game(game_id)
        if result:
            return result

        return Response(status_code=status.HTTP_404_NOT_FOUND)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.put("/games/{game_id}")
async def update_game(game_id: int, request: CreateGameRequest, user: str = Depends(auth)):
    try:
        results = calculate_all_distances_fixed(request.player_id, request.leagues)

        db_handler = FootballDBHandler()
        old_game_number = \
            db_handler.update_game(game_id=game_id, activate_at=parse_datetime(request.activate_at), distance=results, hint=request.hint,
                                   leagues=request.leagues)

        print("Revoking game number: ", old_game_number)
        game_service.revoke_game(game_id=old_game_number)
        game_service.revoke_ranks_for_game(game_id=old_game_number)

        return Response(status_code=status.HTTP_200_OK)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
