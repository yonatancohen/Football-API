from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(env_path)

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from game.limiter import LimiterMiddleware
from game.services.consumer import router as consumer_router
from game.services.admin import router as admin_router
from game.config import auth, game_service

app = FastAPI()
LimiterMiddleware(app)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

app.include_router(consumer_router)
app.include_router(admin_router)

