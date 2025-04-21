import requests
import sqlite3
import json
import time

# ===================================================
# API Client: Handles communication with API‑Football
# ===================================================
import requests

from utils import get_all_players, calculate_all_distances_fixed


class SportMonksAPIClient:
    def __init__(self, api_token, base_url="https://api.sportmonks.com/v3/football/"):
        self.api_token = api_token
        self.base_url = base_url

    def _get(self, endpoint, params=None):
        if params is None:
            params = {}
        params['api_token'] = self.api_token

        all_data = []
        is_specific_page = params.get('page', None) is not None
        page = int(params.get('page', 1))
        per_page = 25  # todo: change to 50?

        while True:
            params['page'] = page
            params['per_page'] = per_page

            url = self.base_url + endpoint
            response = requests.get(url, params=params)

            if response.status_code != 200:
                print(f"Error: {response.status_code} - {response.text}")
                break

            json_data = response.json()
            data = json_data.get('data', [])
            pagination = json_data.get('pagination', {})

            if not data:
                break

            if isinstance(data, list):
                all_data.extend(data)
            else:
                all_data = data

            if not pagination.get('has_more', False) or is_specific_page:
                break

            page += 1
            time.sleep(2)

        return all_data

    def get_leagues_by_country(self, country_id):
        """Returns all football leagues for a given country (e.g. Israel = 802)"""
        return self._get(f"leagues/countries/{country_id}")

    def get_seasons_by_league(self, league_id):
        """Returns all seasons available for a given league using the leagues endpoint."""
        return self._get(f"leagues/{league_id}", {"include": "seasons"})

    def get_teams_by_season(self, season_id):
        """Returns all teams that participated in a specific season"""
        return self._get(f"teams/seasons/{season_id}", {"include": "country"})

    def get_players_by_season_team(self, season_id, team_id):
        """Returns players in a specific team"""
        return self._get(f"squads/seasons/{season_id}/teams/{team_id}", {
            "include": "player;player.nationality;position;details.type",
            "filters": "playerstatisticdetailTypes:40"
        })

    def get_all_players(self):
        return self._get(f"players", {
            "page": "1",
            "include": "nationality;country;position;teams;transfers;statistics;"
                       "transfers.toteam;transfers.toteam.country;transfers.fromteam;transfers.fromteam.country;"
                       "teams.team",
            "filters": "playerCountries:802",
        })

    def get_player_by_id(self, player_id):
        # 123742 - eran
        return self._get(f"players/{player_id}", {
            "include": "teams;transfers;statistics;country;nationality;position;transfers.toteam;transfers.toteam.country;transfers.fromteam;transfers.fromteam.country;teams.team",
        })


# ====================================================
# Database Handler: Creates tables and loads data
# ====================================================
class FootballDBHandler:
    def __init__(self, db_filename):
        self.conn = sqlite3.connect(db_filename)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.create_tables()

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

    def populate_database(self, api_client, conn, league_id):
        cursor = conn.cursor()

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

    def get_players(self, api_client, conn):
        players = api_client.get_all_players()
        print(json.dumps(players))

    def close(self):
        self.conn.close()


# ===========================================
# Main: Fetch data from API and load to DB
# ===========================================
def main():
    ds = get_all_players()
    # ds = calculate_all_distances_fixed(123742)
    print(json.dumps(ds))
    return

    # Configuration Parameters
    API_KEY = "iv5k1aO9nQc2tFeFZ4hNTiLUHgk3y5y4cVxmgoLqAv0JKspGVuxkKPnSpvEm"  # Replace with your API-Football API key
    DB_FILENAME = "football_data.db"

    # Step 1: Fetch Data from API
    api_client = SportMonksAPIClient(API_KEY)

    # Maccabi TLV - 2997; israel season - 23660

    db_handler = FootballDBHandler(DB_FILENAME)
    # db_handler.populate_database(api_client, db_handler.conn, 372)  # ליגת העל
    # db_handler.populate_database(api_client, db_handler.conn, 375)  # ליגה לאומית
    # db_handler.populate_database(api_client, db_handler.conn, 564)  # La Liga
    # db_handler.populate_database(api_client, db_handler.conn, 8)  # Premier League
    # db_handler.populate_database(api_client, db_handler.conn, 82)  # Bundesliga
    # db_handler.populate_database(api_client, db_handler.conn, 301)  # Ligue 1
    # db_handler.populate_database(api_client, db_handler.conn, 384)  # Serie A
    db_handler.close()


if __name__ == "__main__":
    main()
