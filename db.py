import sqlite3
import pandas as pd


class FootballDBHandler:
    def __init__(self, db_filename="football_data.db"):
        self.db_filename = db_filename
        self.conn = sqlite3.connect(self.db_filename)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.create_tables()

        self.conn.close()

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

        self.conn.commit()
        self.conn.close()

    def populate_database(self, api_client, league_id):
        self.conn = sqlite3.connect(self.db_filename)
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

            conn.commit()

        self.conn.close()

    def get_players_for_translate(self):
        self.conn = sqlite3.connect(self.db_filename)
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
        self.conn.close()

        return df

    def get_json_players(self):
        self.conn = sqlite3.connect(self.db_filename)

        all_players = \
            pd.read_sql_query("SELECT id,"
                              "CASE WHEN display_name GLOB '[A-Z]. *' THEN first_name_he || ' ' || last_name_he "
                              "ELSE display_name_he END AS name, "
                              "REPLACE(image, 'https://cdn.sportmonks.com/images/soccer/', '') AS image "
                              "FROM Players", self.conn)
        self.conn.close()

        players_json = all_players.to_dict(orient="records")

        return players_json

    def update_player(self, player_id, first_name, last_name, display_name):
        cursor = self.conn.cursor()

        cursor.execute("""
            UPDATE Players
            SET first_name_he = ?, last_name_he = ?, display_name_he = ?
            WHERE id = ?
        """, (first_name, last_name, display_name, player_id))

        self.conn.commit()
        self.conn.close()
