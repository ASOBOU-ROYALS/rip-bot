import itertools
import logging
import json as python_json
import os
import time
import traceback
from numbers import Number
from typing import Any, Callable, Dict, List, Tuple, Optional
from urllib.parse import urlparse

from flask import Flask, json, request
from discord_interactions import verify_key_decorator
from celery import group

import tasks.tasks as app_tasks
from db.db import add_death_db, get_tally_db, get_tally_time_db, get_death_db, get_death_by_message_id_db, connect_to_database


app = Flask(__name__)
app.logger.setLevel(logging.INFO)

RIP_BOT_PUBLIC_KEY = os.getenv("RIP_BOT_PUBLIC_KEY")
DATABASE_PATH = os.getenv("DATABASE_PATH")

DEATH_MESSAGE_TEMPLATE = """<@{dead_person_id}> died!
Caption by <@{poster_id}>: \"{caption}\""""
REMOVED_DEATH_MESSAGE_TEMPLATE = """~~<@{dead_person_id}> died!
Caption by <@{poster_id}>: \"{caption}\"~~
Removed by <@{remover_id}>."""
DEATH_MESSAGE_RETRIEVE_TEMPLATE = """<@{dead_person_id}> died on <t:{death_time}:f>!
Caption by <@{poster_id}>: \"{caption}\""""
REMOVING_DEATH_IN_PROGRESS_TEMPLATE = """Removing death {death_message_link} for <@{dead_person_id}>."""
ERROR_MESSAGE = """rip-bot failed to process the command."""


def convert_options_to_map(options: List) -> Dict[str, Any]:
    return {option["name"]: option["value"] for option in options}

def parse_discord_message_url(url: str) -> Optional[Tuple[str, str, str]]:
    parse_result = urlparse(url)
    if parse_result.hostname != "discord.com":
        return None

    if not parse_result.path.startswith("/channel"):
        return None
    
    path_parts = parse_result.path.split("/")
    if len(path_parts) != 5:
        # example URL: https://discord.com/channels/guild/channel/message
        # parts are ("", "channels", "guild", "channel", "message")
        return None
    
    return (path_parts[2], path_parts[3], path_parts[4])

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
        req["channel_id"],
        "",
        options["dead-person"],
        options["caption"],
        python_json.dumps(resolved_attachment),
        image_url,
        int(time.time()),
        req["member"]["user"]["id"],
    )
    conn.commit()
    conn.close()

    (app_tasks.download_image_and_upload_to_s3.s(image_url) | \
        group(
            app_tasks.update_database_with_image.s(rowid),
            app_tasks.update_interaction_with_image.s(interaction_token),
        )).delay()
    
    # technically the message takes time to exist in Discord
    # so this delays the messsage ID fetching for a bit
    app_tasks.update_database_with_message_id.s(rowid, interaction_token).apply_async(countdown=0.2)

    log_object = {
        "event": "add_death",
        "guild_id": req["guild_id"],
        "actor": req["member"]["user"]["id"],
        "channel": req["channel_id"],
        "timestamp": time.time(),
        "victim": options["dead-person"],
    }
    app.logger.info(python_json.dumps(log_object))

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
    interaction_token = req["token"]
    options = convert_options_to_map(req["data"]["options"])
    resolved_attachment = req["data"]["resolved"]["attachments"][options["image"]]
    image_url = resolved_attachment["url"]

    (
        group(
            app_tasks.add_death_to_db.s(
                req["guild_id"],
                req["channel_id"],
                "",
                options["dead-person"],
                options["caption"],
                python_json.dumps(resolved_attachment),
                image_url,
                int(time.time()),
                req["member"]["user"]["id"],
            ),
            app_tasks.download_image_and_upload_to_s3.s(image_url),
        ) |
        app_tasks.gather_results.s(interaction_token=interaction_token) |
        group(
            app_tasks.update_database_with_image.s(),
            app_tasks.update_interaction_with_image.s(),
            # technically the message takes time to exist in Discord
            # so this delays the messsage ID fetching for a bit
            # TODO: readd the countdown/delay once I figure out how
            # for now this should be fine since we wait are downloding and uploading a whole image
            # in the first step, which serves as the delay
            app_tasks.update_database_with_message_id.s()
        )).delay()
    
    log_object = {
        "event": "add_death",
        "guild_id": req["guild_id"],
        "actor": req["member"]["user"]["id"],
        "channel": req["channel_id"],
        "timestamp": time.time(),
        "victim": options["dead-person"],
    }
    app.logger.info(python_json.dumps(log_object))

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


def remove_death(req: Any):
    options = convert_options_to_map(req["data"]["options"])
    death_message_link = options.get("death-message-link", None)

    parsed_death_message_url = parse_discord_message_url(death_message_link)
    if not parsed_death_message_url:
        return {
            "type": 4,
            "data": {
                "content": "Invalid Discord message link.",
            },
        }

    guild_id, channel_id, message_id = parsed_death_message_url

    if req["guild_id"] != guild_id:
        return {
            "type": 4,
            "data": {
                "content": "Discord message link is not for this server."
            }
        }

    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()

    death = get_death_by_message_id_db(cursor, message_id)
    conn.close()

    if not death:
        return {
            "type": 4,
            "data": {
                "content": f"Death not found."
            }
        }
    
    rowid, database_guild_id, channel_id, dead_person, caption, reporter = death

    if req["guild_id"] != database_guild_id:
        return {
            "type": 4,
            "data": {
                "content": "Discord message link is not for this server."
            }
        }

    new_message = REMOVED_DEATH_MESSAGE_TEMPLATE.format(
        dead_person_id=dead_person,
        poster_id=reporter,
        caption=caption,
        remover_id=req["member"]["user"]["id"],
    )
    
    (app_tasks.delete_from_database.s(rowid) | \
        app_tasks.update_death_message.si(channel_id, message_id, new_message)
    ).delay()

    log_object = {
        "event": "remove_death",
        "guild_id": req["guild_id"],
        "actor": req["member"]["user"]["id"],
        "channel": req["channel_id"],
        "timestamp": time.time(),
        "message_id": message_id,
    }
    app.logger.info(python_json.dumps(log_object))

    return {
        "type": 4,
        "data": {
            "content": REMOVING_DEATH_IN_PROGRESS_TEMPLATE.format(
                death_message_link=death_message_link,
                dead_person_id=dead_person,
            ),
        },
    }


def tally_deaths(req: Any):
    options = convert_options_to_map(req["data"].get("options", {}))
    start_time, end_time = options.get("start-time", None), options.get("end-time", None)

    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()

    if start_time and end_time:
        try:
            start_time_p, end_time_p = time.mktime(time.strptime(start_time, "%Y-%m-%d")), time.mktime(time.strptime(end_time, "%Y-%m-%d"))
            result = get_tally_time_db(cursor, req["guild_id"], start_time_p, end_time_p)
        except ValueError:
            return {
                "type": 4,
                "data": {
                    "content": "Both start time and end time are required to be yyyy-mm-dd.",
                }
            }
    elif not start_time and not end_time:
        result = get_tally_db(cursor, req["guild_id"])
    else:
        return {
            "type": 4,
            "data": {
                "content": "Both start time and end time are required to be yyyy-mm-dd.",
            }
        }

    conn.close()

    def sort_by_count(row):
        return row[1]

    result.sort(key=sort_by_count, reverse=True)

    header_text = "Deaths"
    if start_time:
        header_text += f" ({start_time} to {end_time})"

    lines_of_text = [f"**{header_text}**"]
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
    result = get_death_db(cursor, req["guild_id"], options["dead-person"])
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
    "remove-death": remove_death,
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
