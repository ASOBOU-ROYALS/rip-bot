import os
import requests
import sqlite3
import time
import urllib.parse
from typing import Tuple

import boto3
import requests
from celery import Celery

from db.db import connect_to_database, update_death_image_url_db

app = Celery("tasks", broker="amqp://localhost")

DATABASE_PATH = os.getenv("DATABASE_PATH")
S3_BUCKET = os.getenv("S3_BUCKET")

DISCORD_BOT_APPLICATION_ID = os.getenv("DISCORD_BOT_APPLICATION_ID")
AUTHORIZATION = os.getenv("AUTHORIZATION")

@app.task
def download_image_and_upload_to_s3(source_url: str) -> Tuple[str, str, str]:
    s3 = boto3.resource("s3")

    image_name = os.path.basename(urllib.parse(source_url).path)
    timestamp = time.time()
    file_name = f"{timestamp}-{image_name}"
    key = f"img/{file_name}"

    response = requests.get(source_url)
    object = s3.Bucket(S3_BUCKET).put_object(
        Key=key,
        Body=response.content,
        ContentType=response.headers["content-type"],
        ACL="public-read",
    )
    
    # b: bucket
    # k: key
    def calculate_s3_url(b, k):
        return f"https://${b}.s3.ca-central-1.amazonaws.com/${k}"

    return file_name, response.content, calculate_s3_url(S3_BUCKET, key)

@app.task
def update_database_with_image(new_file_info: Tuple[str, str, str], rowid: int):
    _, _, new_url = new_file_info

    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()

    update_death_image_url_db(cursor, rowid, new_url)
    conn.commit()
    conn.close()


# update_interaction_with_image is chained from download_image_and_upload_to_s3,
# so file_name, image_content and new_url has to be first
@app.task
def update_interaction_with_image(new_file_info: Tuple[str, str, str], interaction_id: str):
    file_name, image_content, _ = new_file_info

    requests.patch(
        f"https://discord.com/api/v10/webhooks/${DISCORD_BOT_APPLICATION_ID}/${interaction_id}/messages/@original",
        json={
            "attachments": [
                {
                    "id": 0,
                    "filename": file_name,
                }
            ]
        },
        headers={"Authorization": AUTHORIZATION},
        files={
            "files[0]": image_content,
        },
    )