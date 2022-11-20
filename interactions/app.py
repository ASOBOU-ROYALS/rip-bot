from unicodedata import numeric
from json import dumps as json_dumps
from numbers import Number
from typing import Dict, Callable

from flask import Flask, json
import os

from flask import request

app = Flask(__name__)

from discord_interactions import verify_key_decorator

RIP_BOT_PUBLIC_KEY = os.getenv("RIP_BOT_PUBLIC_KEY")

def PingHandler(req: object) -> object:
    return {"type": 1}

def ApplicationCommandHandler(req: object) -> object:
    return {
        "type": 4,
        "data": {
           "content": json_dumps(req["data"]),
        }
    }

InteractionsHandlers: Dict[Number, Callable[[object], object]] = {
    1: PingHandler,
    2: ApplicationCommandHandler,
}

@app.post("/interactions")
@verify_key_decorator(RIP_BOT_PUBLIC_KEY)
def interactions_post():
    request_body = request.get_json()
    interaction_type = request_body["type"]
    response = InteractionsHandlers[interaction_type](request_body)
    return json.jsonify(response)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=14625)
