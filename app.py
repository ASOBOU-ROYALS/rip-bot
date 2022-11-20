from unicodedata import numeric
import json
from numbers import Number
from typing import Dict, Callable

from flask import Flask, json
from flask import request

app = Flask(__name__)

def PingHandler(req: object) -> object:
    return {"type": 1}

def ApplicationCommandHandler(req: object) -> object:
    return {
        "type": 4,
        "data": {
            "content": f"Discord gave me this: {json.dumps(req)}",
        }
    }

InteractionsHandlers: Dict[Number, Callable[[object], object]] = {
    1: PingHandler,
    2: ApplicationCommandHandler,
}

@app.post("/interactions")
def interactions_post():
    request_body = request.get_json()
    interaction_type = request_body["type"]
    response = InteractionsHandlers[interaction_type](request_body)
    return json.jsonify(response)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port="5000")