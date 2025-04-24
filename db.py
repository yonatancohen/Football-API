from datetime import datetime
from typing import Optional

import json
import sqlite3
import pandas as pd
import atexit


class FootballDBHandler:
    _instance = None
    _initialized = False

    def __new__(cls, db_filename: str = "football_data.db"):
        # Singleton: only one instance
        if cls._instance is None:
            cls._instance = super(FootballDBHandler, cls).__new__(cls)
        return cls._instance

    def __init__(self, db_filename: str = "football_data.db"):
        if self._initialized:
            # Already initialized, skip table creation
            return

        self.db_filename = db_filename

        # Open connection once
        self.conn = sqlite3.connect(self.db_filename)
        self.conn.execute("PRAGMA foreign_keys = ON")

        # First-time setup
        self.create_tables()

        # Register cleanup on program exit
        atexit.register(self.close)

        # Mark as initialized to prevent re-creating tables
        type(self)._initialized = True

    def create_tables(self):
        cursor = self.conn.cursor()

        # Countries
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Countries (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                image TEXT
            );
        ''')

        # Leagues
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Leagues (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                image TEXT,
                sub_type TEXT
            );
        ''')

        # Seasons
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Seasons (
                id INTEGER PRIMARY KEY,
                league_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                FOREIGN KEY (league_id) REFERENCES Leagues(id)
            );
        ''')

        # Teams
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Teams (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                image TEXT,
                country_id INTEGER,
                FOREIGN KEY (country_id) REFERENCES Countries(id)
            );
        ''')

        # Players
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Players (
                id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                display_name TEXT,
                first_name_he TEXT,
                last_name_he TEXT,
                display_name_he TEXT,
                image TEXT,
                date_of_birth TEXT,
                height INTEGER,
                weight INTEGER,
                nationality_id INTEGER,
                FOREIGN KEY (nationality_id) REFERENCES Countries(id)
            );
        ''')

        # Positions
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Positions (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
        ''')

        # Player-Team-Season link
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS PlayerTeamSeason (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER NOT NULL,
                team_id INTEGER NOT NULL,
                season_id INTEGER NOT NULL,
                position_id INTEGER,
                shirt_number INTEGER,
                is_captain BOOLEAN,
                FOREIGN KEY (player_id) REFERENCES Players(id),
                FOREIGN KEY (team_id) REFERENCES Teams(id),
                FOREIGN KEY (season_id) REFERENCES Seasons(id),
                FOREIGN KEY (position_id) REFERENCES Positions(id)
            );
        ''')

        # Games
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Games (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
                activate_at  DATETIME,
                distance     JSON,
                max_rank    INTEGER
                hint        TEXT
                leagues     JSON
                players     JSON
            );
        ''')

        self.conn.commit()

    def populate_database(self, api_client, league_id):
        cursor = self.conn.cursor()

        league_seasons = api_client.get_seasons_by_league(league_id)

        # --- Insert League ---
        league_id = league_seasons['id']
        league_name = league_seasons['name']
        league_image = league_seasons['image_path']
        sub_type = league_seasons['sub_type']

        cursor.execute("""
            INSERT OR IGNORE INTO Leagues (id, name, image, sub_type)
            VALUES (?, ?, ?, ?)
        """, (league_id, league_name, league_image, sub_type))

        # --- Iterate over Seasons ---
        for season in sorted(league_seasons['seasons'], key=lambda x: x["starting_at"], reverse=True):
            season_id = season['id']
            season_name = season['name']

            print(f"Season {season_name}")
            print("------------------------------")

            cursor.execute("""
                INSERT OR IGNORE INTO Seasons (id, league_id, name)
                VALUES (?, ?, ?)
            """, (season_id, league_id, season_name))

            teams = api_client.get_teams_by_season(season_id)
            for team in teams:
                team_id = team['id']
                team_name = team['name']
                team_image = team['image_path']

                country = team['country']
                country_id = country['id']
                country_name = country['name']
                country_image = country['image_path']

                cursor.execute("""
                    INSERT OR IGNORE INTO Countries (id, name, image)
                    VALUES (?, ?, ?)
                """, (country_id, country_name, country_image))

                cursor.execute("""
                    INSERT OR IGNORE INTO Teams (id, name, image, country_id)
                    VALUES (?, ?, ?, ?)
                """, (team_id, team_name, team_image, country_id))

                players = api_client.get_players_by_season_team(season_id, team_id)

                print(f"{team_name} / {len(players)} players")

                if players:
                    for team_player in players:
                        player = team_player['player']
                        player_id = player['id']
                        first_name = player['firstname']
                        last_name = player['lastname']
                        display_name = player.get('display_name')
                        player_image = player.get('image_path', None)
                        dob = player['date_of_birth']
                        height = player.get('height', None)
                        weight = player.get('weight', None)

                        nationality = player['nationality']
                        if nationality:
                            nat_id = nationality['id']
                            nat_name = nationality['name']
                            nat_image = nationality['image_path']

                            cursor.execute("""
                                INSERT OR IGNORE INTO Countries (id, name, image)
                                VALUES (?, ?, ?)
                            """, (nat_id, nat_name, nat_image))
                        else:
                            nat_id = None

                        cursor.execute("""
                            INSERT OR IGNORE INTO Players (id, first_name, last_name, display_name, image, date_of_birth, height, weight, nationality_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (player_id, first_name, last_name, display_name, player_image, dob, height, weight, nat_id))

                        position = team_player['position']
                        if position:
                            pos_id = position['id']
                            pos_name = position['name']

                            cursor.execute("""
                                INSERT OR IGNORE INTO Positions (id, name)
                                VALUES (?, ?)
                            """, (pos_id, pos_name))
                        else:
                            pos_id = None

                        jersey_number = team_player.get('jersey_number')

                        is_captain = any(
                            detail.get("type", {}).get("code") == "captain"
                            for detail in team_player.get('details', None)
                        )

                        cursor.execute("""
                        INSERT INTO PlayerTeamSeason (
                            player_id, team_id, season_id, position_id, shirt_number, is_captain)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (player_id, team_id, season_id, pos_id, jersey_number, is_captain))
            print("\n")

            self.conn.commit()

    def get_players_for_translate(self):
        query = """
                   SELECT DISTINCT p.id,
                       p.display_name, 
                       p.first_name, 
                       p.last_name,
                       latest_pts.shirt_number,
                       t.name AS team_name,
                       p.nationality_id
                   FROM Players p
                   INNER JOIN (
                       SELECT player_id, MAX(season_id) AS latest_season_id, team_id, shirt_number
                       FROM PlayerTeamSeason
                       GROUP BY player_id
                   ) latest_pts ON p.id = latest_pts.player_id
                   INNER JOIN Teams t ON t.id = latest_pts.team_id
                   WHERE (p.display_name_he IS NULL OR p.display_name_he = '')
                   ORDER BY p.id,p.display_name
               """
        df = pd.read_sql_query(query, self.conn)

        return df

    def get_autocomplete_players(self, leagues_id=None, player_name=None):
        query = """
                    SELECT DISTINCT(P.id),
                           CASE WHEN display_name GLOB '[A-Z]. *' THEN first_name_he || ' ' || last_name_he 
                                ELSE display_name_he END AS name 
                           --,REPLACE(image, 'https://cdn.sportmonks.com/images/soccer/', '') AS image 
                    FROM Players P
                """

        params = []
        if player_name:
            query += " WHERE name LIKE ?"
            params.append(f"%{player_name}%")

        if leagues_id:
            placeholders = ",".join(["?"] * len(leagues_id))
            query += """
                        INNER JOIN PlayerTeamSeason PS ON PS.player_id = P.id
                        INNER JOIN Seasons S ON S.id = PS.season_id AND S.league_id IN ({})
                    """.format(placeholders)
            params.extend(leagues_id)

        all_players = pd.read_sql_query(query, self.conn, params=params)
        return all_players.to_dict(orient="records")

    def get_player(self, player_id):
        query = """SELECT * FROM PLAYERS WHERE ID = ?"""
        player = pd.read_sql_query(query, self.conn, params=[player_id])
        return player.to_dict(orient="records")

    def update_player(self, player_id, first_name, last_name, display_name):
        cursor = self.conn.cursor()

        cursor.execute("""
            UPDATE Players
            SET first_name_he = ?, last_name_he = ?, display_name_he = ?
            WHERE id = ?
        """, (first_name, last_name, display_name, player_id))

        self.conn.commit()

    def get_game(self, game_id: Optional[int]):
        if game_id:
            game = pd.read_sql_query(f"SELECT id, created_at, activate_at, distance, max_rank, hint, players FROM Games WHERE id = {game_id}", self.conn)
        else:
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            game = pd.read_sql_query(f"SELECT id, created_at, activate_at, distance, max_rank, hint, players FROM Games WHERE activate_at <= '{now}' "
                                     "ORDER BY activate_at DESC LIMIT 1", self.conn)
        if not game.empty:
            return game.iloc[0].to_dict()

        return None

    def get_player_rank(self, game_id: int, player_id: int) -> int | None:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        df_game = pd.read_sql_query("SELECT distance FROM Games WHERE id = ? AND activate_at < ?", self.conn, params=(game_id, now))

        if df_game.empty or pd.isna(df_game.loc[0, "distance"]):
            return None

        try:
            df_distance = pd.DataFrame(json.loads(df_game.loc[0, "distance"]))
            result = df_distance.loc[df_distance["id"] == player_id, "rank"]
            if not result.empty:
                return int(result.iloc[0])
        except Exception:
            return None

        return None

    def create_game(self, activate_at: str, distance, hint: str, leagues):
        cursor = self.conn.cursor()

        max_rank = max(item["rank"] for item in distance)

        players_search = self.get_autocomplete_players(leagues_id=leagues)

        cursor.execute("""
            INSERT INTO Games (activate_at, distance, max_rank, hint, leagues, players)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (activate_at, json.dumps(distance), max_rank, hint, json.dumps(leagues, ensure_ascii=False),
                  json.dumps(players_search, ensure_ascii=False)))

        self.conn.commit()

    def get_leagues(self):
        leagues = pd.read_sql_query("SELECT ID, NAME FROM LEAGUES ORDER BY NAME", self.conn)
        return leagues.to_dict(orient="records")

    def get_countries(self):
        countries = pd.read_sql_query("SELECT ID, NAME FROM COUNTRIES ORDER BY NAME", self.conn)
        return countries.to_dict(orient="records")

    def close(self):
        if hasattr(self, 'conn') and self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        # Reset singleton state
        type(self)._instance = None
        type(self)._initialized = False

    def __del__(self):
        # Ensure connection is closed when instance is destroyed
        self.close()
