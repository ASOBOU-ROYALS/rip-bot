import itertools
import json as python_json
import os
import sqlite3
import time
import traceback
from numbers import Number
from typing import Any, Callable, Dict, List, Tuple

from flask import Flask, json, request
from discord_interactions import verify_key_decorator
from celery import group

from db.db import add_death_db, get_tally_db, get_death_db, connect_to_database
from tasks.tasks import download_image_and_upload_to_s3, update_database_with_image, update_interaction_with_image

app = Flask(__name__)


RIP_BOT_PUBLIC_KEY = os.getenv("RIP_BOT_PUBLIC_KEY")
DATABASE_PATH = os.getenv("DATABASE_PATH")

DEATH_MESSAGE_TEMPLATE = """<@{dead_person_id}> died!
Caption by <@{poster_id}>: \"{caption}\""""
DEATH_MESSAGE_RETRIEVE_TEMPLATE = """<@{dead_person_id}> died on <t:{death_time}:f>!
Caption by <@{poster_id}>: \"{caption}\""""
ERROR_MESSAGE = """rip-bot failed to process the command."""


def convert_options_to_map(options: List) -> Dict[str, Any]:
    return {option["name"]: option["value"] for option in options}


def PingHandler(req: Any) -> Any:
    return {"type": 1}


def add_death(req: Any):
    interaction_token = req["token"]
    options = convert_options_to_map(req["data"]["options"])
    resolved_attachment = req["data"]["resolved"]["attachments"][options["image"]]
    image_url = resolved_attachment["url"]

    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()
    rowid = add_death_db(
        cursor,
        req["guild_id"],
        options["dead-person"],
        options["caption"],
        python_json.dumps(resolved_attachment),
        image_url,
        int(time.time()),
        req["member"]["user"]["id"],
        req["id"],
    )
    conn.commit()
    conn.close()

    (download_image_and_upload_to_s3.s(image_url) | \
        group(
            update_database_with_image.s(rowid),
            update_interaction_with_image.s(interaction_token),
        )).delay()

    return {
        "type": 4,
        "data": {
            "content": DEATH_MESSAGE_TEMPLATE.format(
                dead_person_id=options["dead-person"],
                caption=options["caption"],
                poster_id=req["member"]["user"]["id"],
            )
        }
    }


def add_death_beta(req: Any):
    return add_death(req)


def tally_deaths(req: Any):
    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()
    result = get_tally_db(cursor)
    conn.commit()
    conn.close()

    def sort_by_count(row):
        return row[1]

    result.sort(key=sort_by_count, reverse=True)

    lines_of_text = ["**Deaths**"]
    current_rank = 1
    for dead_person, death_count in itertools.islice(result, 50):
        lines_of_text.append(f"{current_rank}. <@{dead_person}> - {death_count}")
        current_rank += 1

    content = "\n".join(lines_of_text)

    return {
        "type": 4,
        "data": {
            "content": content,
        }
    }


def get_death(req: Any):
    options = convert_options_to_map(req["data"]["options"])

    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()
    result = get_death_db(cursor, options["dead-person"])
    conn.commit()
    conn.close()

    return {
        "type": 4,
        "data": {
            "content": DEATH_MESSAGE_RETRIEVE_TEMPLATE.format(
                dead_person_id=options["dead-person"],
                caption=result["caption"],
                death_time=result["timestamp"],
                poster_id=result["reporter"],
            ),
            "embeds": [
                {
                    "type": "image",
                    "image": python_json.loads(result["attachment"]),
                },
            ],
        }
    }

SlashCommandHandlers: Dict[str, Callable[[Any], Any]] = {
    "add-death": add_death,
    "add-death-beta": add_death_beta,
    "get-death": get_death,
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
        return json.jsonify({
            "type": 4,
            "data": {
                "content": ERROR_MESSAGE + "Technobabble: ||" + traceback.format_exc() + "||"
            }
        })

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=14625)
