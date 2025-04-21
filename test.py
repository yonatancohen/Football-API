import sqlite3
import pandas as pd


def calculate_all_distances_fixed(target_player_id, db_path="football_data.db"):
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

    conn = sqlite3.connect(db_path)

    # Get all player IDs
    all_players_df = pd.read_sql_query("SELECT id FROM Players", conn)
    all_player_ids = all_players_df["id"].tolist()

    # Preload all player profiles
    profiles = {}
    for pid in all_player_ids:
        query = f"""
        SELECT P.id, P.date_of_birth, P.nationality_id, PS.team_id, PS.season_id,
               PS.position_id, PS.shirt_number, PS.is_captain, P.first_name, P.last_name
        FROM Players P
        JOIN PlayerTeamSeason PS ON P.id = PS.player_id
        WHERE P.id = {pid}
        """
        profiles[pid] = pd.read_sql_query(query, conn)

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
            "rank": 1,
            "name": f"{target_profile.first_name.values[0]} f{target_profile.last_name.values[0]}"
        }
    }
    for rank, (pid, _) in enumerate(sorted_scores, start=2):
        distance_map[pid] = {
            "id": pid,
            "rank": rank,
            "name": f"{profiles[pid].first_name.values[0]} f{profiles[pid].last_name.values[0]}"
        }

    conn.close()
    return list(distance_map.values())
