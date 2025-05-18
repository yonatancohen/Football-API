# Guess Game

In order to start FastAPI, run the following command:
```bash
    python3 -m uvicorn api:app --reload --log-level debug
```

## Development ##
.env file should contain the following variables:

```
DATABASE_URL=postgresql://yonicohen@localhost:5432/guess_game_db
```

## Production ##
.env file should contain the following variables:

```
postgresql+psycopg2://guess_game_user:guess_game_AY_password#@db:5432/guess_game_db
```



