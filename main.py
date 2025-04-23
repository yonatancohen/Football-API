import json

from db import FootballDBHandler
from sportmonks import SportMonksAPIClient
from translate import translate_db
from utils import calculate_all_distances_fixed


def get_api_data():
    # Step 1: Fetch Data from API
    api_client = SportMonksAPIClient()

    # Maccabi TLV - 2997; israel season - 23660

    db_handler = FootballDBHandler()
    db_handler.populate_database(api_client, 372)  # ליגת העל
    db_handler.populate_database(api_client, 375)  # ליגה לאומית
    db_handler.populate_database(api_client, 564)  # La Liga
    db_handler.populate_database(api_client, 8)  # Premier League
    db_handler.populate_database(api_client, 82)  # Bundesliga
    db_handler.populate_database(api_client, 301)  # Ligue 1
    db_handler.populate_database(api_client, 384)  # Serie A


if __name__ == "__main__":
    # db_handler = FootballDBHandler()
    # a = json.dumps(db_handler.get_json_players(), ensure_ascii=False)
    # ds = calculate_all_distances_fixed(123742)

    # get_api_data()
    pass
