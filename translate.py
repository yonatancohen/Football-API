import time
import openai
import re
import math

from db import FootballDBHandler

OPEN_API_KEY = ''


def translate_full_name_via_gpt(first_name, last_name, team_name="", shirt_number=""):
    full_name = f"{first_name} {last_name}".strip()
    context = f"He plays for {team_name}" if team_name else ""
    if shirt_number:
        context += f" and wears jersey number {shirt_number}"

    prompt = (
        f"Translate the football player's name '{full_name}' to Hebrew. {context}. "
        f"Use common Israeli sports media conventions."
    )

    try:
        client = openai.OpenAI(api_key=OPEN_API_KEY)
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )
        return response.choices[0].message.content.strip().replace('"', "")
    except Exception as e:
        print(f"❌ Error translating '{full_name}': {e}")
        return ""


def create_display_name_he(display_name, first_name_he, last_name_he):
    def get_first_hebrew_letter(text):
        for char in text:
            if '\u0590' <= char <= '\u05FF':
                return char
        return ""

    match = re.match(r"^(\w)\.?\s+([^\s]+)$", display_name)
    if match:
        return f"{get_first_hebrew_letter(first_name_he)}. {last_name_he}"
    return f"{first_name_he} {last_name_he}"


def translate_db():
    db_handler = FootballDBHandler()
    df = db_handler.get_players_for_translate()

    print("🔁 Translating rows...")
    for index, row in df.iterrows():
        player_id = row.get("id", "")
        first = row.get("first_name", "")
        last = row.get("last_name", "")
        display = row.get("display_name", "")
        team = row.get("team_name", "")
        shirt = row.get("shirt_number", "")
        if isinstance(shirt, float) and math.isnan(shirt):
            shirt = ''

        full_he = translate_full_name_via_gpt(first, last, team, shirt)

        if " " in full_he:
            parts = full_he.split(" ", 1)
            first_he, last_he = parts[0], parts[1]
        else:
            first_he, last_he = full_he, ""

        display_he = create_display_name_he(display, first_he, last_he)

        db_handler.update_player(player_id, first_he, last_he, display_he)

        print(f"✅ {display} → {display_he}/{first_he} {last_he}")
        time.sleep(.7)
