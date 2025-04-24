import sqlite3
from datetime import datetime

import pandas as pd

DB_PATH = "football_data.db"


def calculate_all_distances_fixed(target_player_id, leagues_id=None):
    def score_profiles(profile1, profile2):
        score = 0.0

        team_season_1 = set(zip(profile1["team_id"], profile1["season_id"]))
        team_season_2 = set(zip(profile2["team_id"], profile2["season_id"]))
        shared_team_seasons = team_season_1 & team_season_2
        score += 0.15 * len(shared_team_seasons)

        teams1 = set(profile1["team_id"])
        teams2 = set(profile2["team_id"])
        shared_teams = teams1 & teams2
        if shared_teams and not shared_team_seasons:
            score += 0.05

        league1 = set(profile1["league_id"])
        league2 = set(profile2["league_id"])
        shared_league = league1 & league2
        if shared_league:
            score += 0.15

        positions1 = set(profile1["position_id"])
        positions2 = set(profile2["position_id"])
        if positions1 & positions2:
            score += 0.10

        nat1 = profile1["nationality_id"].iloc[0] if not profile1.empty else None
        nat2 = profile2["nationality_id"].iloc[0] if not profile2.empty else None
        if nat1 and nat2 and nat1 == nat2:
            score += 0.07

        if profile1["is_captain"].any() and profile2["is_captain"].any():
            score += 0.03

        shirts1 = set(profile1["shirt_number"].dropna())
        shirts2 = set(profile2["shirt_number"].dropna())
        if shirts1 & shirts2:
            score += 0.02

        try:
            yob1 = int(profile1["date_of_birth"].iloc[0][:4])
            yob2 = int(profile2["date_of_birth"].iloc[0][:4])
            if abs(yob1 - yob2) <= 2:
                score += 0.03
        except Exception:
            pass

        return round(score, 6)

    conn = sqlite3.connect(DB_PATH)

    # Get all player IDs
    first_query = """
        SELECT DISTINCT(P.id) 
        FROM Players P
    """
    if leagues_id is not None and len(leagues_id) > 0:
        placeholders = ",".join(["?"] * len(leagues_id))
        first_query += f" INNER JOIN PlayerTeamSeason PS ON PS.PLAYER_ID = P.ID INNER JOIN Seasons S on S.id = PS.season_id " \
                       f"AND S.league_id IN ({placeholders}) "
    all_players_df = pd.read_sql_query(first_query, conn, params=leagues_id)

    all_player_ids = all_players_df["id"].tolist()

    query = f"""
        SELECT DISTINCT(P.id), P.date_of_birth, P.nationality_id, PS.team_id, PS.season_id,
               PS.position_id, PS.shirt_number, PS.is_captain, S.league_id
        FROM Players P
        JOIN PlayerTeamSeason PS ON P.id = PS.player_id
    """
    if leagues_id is not None and len(leagues_id) > 0:
        placeholders = ",".join(["?"] * len(leagues_id))
        query += f" INNER JOIN Seasons S on S.id = PS.season_id AND S.league_id IN ({placeholders}) "

    placeholders = ",".join(["?"] * len(all_player_ids))
    query += f"WHERE P.id IN ({placeholders})"

    df = pd.read_sql_query(query, conn, params=leagues_id + all_player_ids)

    # Optional: split into dictionary by player ID (if needed)
    profiles = {pid: df[df["id"] == pid] for pid in all_player_ids}

    target_profile = profiles[target_player_id]
    similarity_scores = {}

    for pid in all_player_ids:
        if pid == target_player_id:
            continue  # exclude for now, we'll insert it as rank 1 later
        similarity_scores[pid] = score_profiles(target_profile, profiles[pid])

    # Sort by descending score
    sorted_scores = sorted(similarity_scores.items(), key=lambda x: -x[1])

    # Assign unique ranks, starting from 2
    distance_map = {
        target_player_id: {
            "id": target_player_id,
            "rank": 1
        }
    }
    for rank, (pid, _) in enumerate(sorted_scores, start=2):
        distance_map[pid] = {
            "id": pid,
            "rank": rank
        }

    conn.close()
    return list(distance_map.values())


def parse_boolean(value):
    if value == 'False' or value == 'false' or value == '0' or value == 0:
        value = False
    elif value:
        value = True
    else:
        value = False

    return value


def parse_datetime(value):
    if '+' in value:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S%z")

    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def parse_date(value):
    return datetime.strptime(value, "%Y-%m-%d")
