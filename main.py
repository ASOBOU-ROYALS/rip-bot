from flask import Flask
from flask import request

app = Flask(__name__)

@app.post("/interactions")
def interactions_post():
    if request.get_json()["type"] == 1:
        return '{"type": 1}'

if __name__ == "__main__":
    app.run(host="127.0.0.1", port="5000")