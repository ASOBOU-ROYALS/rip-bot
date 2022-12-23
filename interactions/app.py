from unicodedata import numeric
from json import dumps as json_dumps
from numbers import Number
from typing import Any, List, Dict, Callable

from flask import Flask, json
import os

from flask import request

app = Flask(__name__)

from discord_interactions import verify_key_decorator

RIP_BOT_PUBLIC_KEY = os.getenv("RIP_BOT_PUBLIC_KEY")

DEATH_MESSAGE_TEMPLATE = """<@{dead_person_id}> died!"""
ERROR_MESSAGE = """rip-bot failed to process the command."""

def convert_options_to_map(options: List) -> Dict[str, Any]:
    return {option["name"]: option["value"] for option in options}

def PingHandler(req: Any) -> Any:
    return {"type": 1}

def ApplicationCommandHandler(req: Any) -> Any:
    options = convert_options_to_map(req["data"]["options"])
    return {
        "type": 4,
        "data": {
            "content": DEATH_MESSAGE_TEMPLATE.format(dead_person_id=options["dead-person"]),
            "embeds": [
                {
                    "title": options["caption"],
                    "type": "image",
                    "image": req["data"]["resolved"]["attachments"][options["image"]],
                },
            ],
        }
    }

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
                "content": ERROR_MESSAGE
            }
        })

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=14625)
