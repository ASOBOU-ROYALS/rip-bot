import base64
import os
import requests
import sqlite3
import time
from urllib.parse import urlparse
from typing import Tuple

import boto3
import requests
from celery import Celery

from db.db import connect_to_database, update_death_image_url_db, update_death_message_id_db, delete_death_db

app = Celery("tasks", broker="amqp://localhost")

DATABASE_PATH = os.getenv("DATABASE_PATH")
S3_BUCKET = os.getenv("S3_BUCKET")

DISCORD_BOT_APPLICATION_ID = os.getenv("DISCORD_BOT_APPLICATION_ID")
AUTHORIZATION = os.getenv("AUTHORIZATION")

@app.task
def download_image_and_upload_to_s3(source_url: str) -> Tuple[str, str, str]:
    s3 = boto3.resource("s3")

    image_name = os.path.basename(urlparse(source_url).path)
    timestamp = time.time()
    file_name = f"{timestamp}-{image_name}"
    key = f"img/{file_name}"

    response = requests.get(source_url)
    content_type = response.headers["content-type"]

    object = s3.Bucket(S3_BUCKET).put_object(
        Key=key,
        Body=response.content,
        ContentType=content_type,
    )
    
    # b: bucket
    # k: key
    def calculate_s3_url(b, k):
        return f"https://{b}.s3.ca-central-1.amazonaws.com/{k}"
    
    encoded_image = base64.b64encode(response.content).decode("utf-8")

    return file_name, content_type, encoded_image, calculate_s3_url(S3_BUCKET, key)

@app.task
def update_database_with_image(new_file_info: Tuple[str, str, str], rowid: int):
    _, _, _, new_url = new_file_info

    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()

    update_death_image_url_db(cursor, rowid, new_url)
    conn.commit()
    conn.close()


# update_interaction_with_image is chained from download_image_and_upload_to_s3,
# so file_name, image_content and new_url has to be first
@app.task
def update_interaction_with_image(new_file_info: Tuple[str, str, str], interaction_token: str):
    file_name, file_content_type, image_content, _ = new_file_info

    response = requests.patch(
        f"https://discord.com/api/v10/webhooks/{DISCORD_BOT_APPLICATION_ID}/{interaction_token}/messages/@original",
        json={
            "attachments": [{"id": 0}]
        },
        headers={"Authorization": AUTHORIZATION},
        files={
            "files[0]": (file_name, base64.b64decode(image_content.encode("utf-8")), file_content_type),
        },
    )
    
    response.raise_for_status()


@app.task
def update_database_with_message_id(rowid: str, interaction_token: str):
    response = requests.get(
        f"https://discord.com/api/v10/webhooks/{DISCORD_BOT_APPLICATION_ID}/{interaction_token}/messages/@original",
        headers={"Authorization": AUTHORIZATION},
    )

    response.raise_for_status()
    message = response.json()

    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()

    update_death_message_id_db(cursor, rowid, message["id"])
    conn.commit()
    conn.close()


@app.task
def delete_from_database(rowid: str):
    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()

    delete_death_db(cursor, rowid)
    conn.commit()
    conn.close()


@app.task
def update_death_message(channel_id: str, message_id: str, new_content: str):
    response = requests.patch(
        f"https://discord.com/api/v10/channels/{channel_id}/messages/{message_id}",
        json={
            "content": new_content,
        },
        headers={"Authorization": AUTHORIZATION},
    )

    response.raise_for_status()