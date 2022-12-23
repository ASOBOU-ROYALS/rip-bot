import json as python_json
import os
import sqlite3
import time
import traceback
from numbers import Number
from typing import Any, Callable, Dict, List, Tuple

from flask import Flask, json, request
from discord_interactions import verify_key_decorator

app = Flask(__name__)


RIP_BOT_PUBLIC_KEY = os.getenv("RIP_BOT_PUBLIC_KEY")
DATABASE_PATH = os.getenv("DATABASE_PATH")

INSERT_DEATH_SQL = """INSERT INTO deaths VALUES (:server, :dead_person, :caption, :attachment, :timestamp, :reporter, :interaction_id)"""
SELECT_DEADPERSON_COUNT_SQL = """SELECT dead_person, COUNT(rowid) FROM deaths GROUP BY dead_person"""

DEATH_MESSAGE_TEMPLATE = """<@{dead_person_id}> died!"""
ERROR_MESSAGE = """rip-bot failed to process the command."""


def connect_to_database() -> sqlite3.Connection:
    return sqlite3.connect(DATABASE_PATH)


def add_death_db(
    cursor: sqlite3.Cursor,
    server: str,
    dead_person: str,
    caption: str,
    attachment: any,
    timestamp: Number,
    reporter: str,
    interaction_id: str,
):
    cursor.execute(
        INSERT_DEATH_SQL,
        {
            "server": server,
            "dead_person": dead_person,
            "caption": caption,
            "attachment": attachment,
            "timestamp": timestamp,
            "reporter": reporter,
            "interaction_id": interaction_id,
        },
    )


def get_tally_db(cursor: sqlite3.Cursor) -> List[Tuple[str, int]]:
    response = cursor.execute(SELECT_DEADPERSON_COUNT_SQL)
    return response.fetchall()


def convert_options_to_map(options: List) -> Dict[str, Any]:
    return {option["name"]: option["value"] for option in options}


def PingHandler(req: Any) -> Any:
    return {"type": 1}


def add_death(req: Any):
    options = convert_options_to_map(req["data"]["options"])
    resolved_attachment = req["data"]["resolved"]["attachments"][options["image"]]

    conn = connect_to_database()
    cursor = conn.cursor()
    add_death_db(
        cursor,
        req["guild_id"],
        options["dead-person"],
        options["caption"],
        python_json.dumps(resolved_attachment),
        int(time.time()),
        req["member"]["user"]["id"],
        req["id"],
    )
    conn.commit()
    conn.close()

    return {
        "type": 4,
        "data": {
            "content": DEATH_MESSAGE_TEMPLATE.format(dead_person_id=options["dead-person"]),
            "embeds": [
                {
                    "title": options["caption"],
                    "type": "image",
                    "image": resolved_attachment,
                },
            ],
        }
    }


def tally_deaths(req: Any):
    conn = connect_to_database()
    cursor = conn.cursor()
    result = get_tally_db(cursor)
    conn.commit()
    conn.close()

    def sort_by_count(row):
        return row[1]

    result.sort(key=sort_by_count, reverse=True)

    lines_of_text = ["**Deaths**"]
    current_rank = 1
    for dead_person, death_count in result:
        lines_of_text.append(f"{current_rank}. <@{dead_person}> - {death_count}")
        current_rank += 1

    content = "\n".join(lines_of_text)

    return {
        "type": 4,
        "data": {
            "content": content,
        }
    }

SlashCommandHandlers: Dict[str, Callable[[Any], Any]] = {
    "add-death": add_death,
    "tally-deaths": tally_deaths,
}

def ApplicationCommandHandler(req: Any) -> Any:
    command_name = req["data"]["name"]
    return SlashCommandHandlers[command_name](req)

InteractionsHandlers: Dict[Number, Callable[[Any], Any]] = {
    1: PingHandler,
    2: ApplicationCommandHandler,
}

@app.post("/interactions")
@verify_key_decorator(RIP_BOT_PUBLIC_KEY)
def interactions_post():
    try:
        request_body = request.get_json()
        interaction_type = request_body["type"]
        response = InteractionsHandlers[interaction_type](request_body)
        return json.jsonify(response)
    except Exception as e:
        print(traceback.format_exc())
        return json.jsonify({
            "type": 4,
            "data": {
                "content": ERROR_MESSAGE
            }
        })

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=14625)
