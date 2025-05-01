import requests
import time


class SportMonksAPIClient:
    def __init__(self, base_url="https://api.sportmonks.com/v3/football/"):
        self.api_token = ""
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

    def get_players(self):
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
