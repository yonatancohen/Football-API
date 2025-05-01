from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from game.config import auth, game_service  # Don't remove - Used by FastAPI
from game.limiter import LimiterMiddleware
from game.services.consumer import router as consumer_router
from game.services.admin import router as admin_router

app = FastAPI()
LimiterMiddleware(app)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

app.include_router(consumer_router)
app.include_router(admin_router)
