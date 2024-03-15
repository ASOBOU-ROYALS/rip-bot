import base64
import os
import requests
import sqlite3
import time
from numbers import Number
from urllib.parse import urlparse
from typing import Any, Dict, List, Tuple

import boto3
import requests
from celery import Celery, Task

from db.db import connect_to_database, add_death_db, update_death_image_url_db, update_death_message_id_db, delete_death_db

CELERY_BROKER = os.getenv("CELERY_BROKER")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND")
# just make sure it's defined, we don't need to pass it in below manually
if not CELERY_RESULT_BACKEND:
    raise ValueError("Missing CELERY_RESULT_BACKEND value.")

app = Celery("tasks", broker=CELERY_BROKER)

DATABASE_PATH = os.getenv("DATABASE_PATH")
S3_BUCKET = os.getenv("S3_BUCKET")

DISCORD_BOT_APPLICATION_ID = os.getenv("DISCORD_BOT_APPLICATION_ID")
AUTHORIZATION = os.getenv("AUTHORIZATION")


@app.task
def add_death_to_db(
    server: str,
    channel_id: str,
    message_id: str,
    dead_person: str,
    caption: str,
    attachment: str,
    image_url: str,
    timestamp: Number,
    reporter: str,
) -> int:
    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()

    rowid = add_death_db(
        cursor,
        server,
        channel_id,
        message_id,
        dead_person,
        caption,
        attachment,
        image_url,
        timestamp,
        reporter,
    )
    conn.commit()
    conn.close()

    return { "rowid": rowid }
    

@app.task
def download_image_and_upload_to_s3(source_url: str) -> Dict:
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
    
    encoded_image = base64.b64encode(response.content).decode("utf-8")
    s3_url = f"https://{S3_BUCKET}.s3.ca-central-1.amazonaws.com/{key}"

    return { "image": (file_name, content_type, encoded_image, s3_url) }

# combines results from a Celery group into a Dict to passed to future Tasks as a single Dict
@app.task
def gather_results(results: List[Dict], **kwargs) -> Dict:
    for task_result in results:
        for key in task_result.keys():
            if key in kwargs:
                raise ValueError(f"duplicate key encountered: {key}")
            
            kwargs[key] = task_result[key]
    
    return kwargs


@app.task
def update_database_with_image(input: Dict):
    rowid: int = input.get("rowid", None)
    image = input.get("image", None)

    if not rowid or not image:
        raise ValueError("missing argument")
    
    _, _, _, new_url = image
    if not new_url:
        raise ValueError("missing image field")

    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()

    update_death_image_url_db(cursor, rowid, new_url)
    conn.commit()
    conn.close()


# update_interaction_with_image is chained from download_image_and_upload_to_s3,
# so file_name, image_content and new_url has to be first
@app.task
def update_interaction_with_image(input: Dict):
    image: Tuple[str, str, str, str] = input.get("image", None)

    if not image:
        raise ValueError("missing image")
    
    file_name, file_content_type, image_content, _ = image
    if not file_name or not file_content_type or not image_content:
        raise ValueError("missing image field")

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


def update_database_with_message_id(input: Dict):
    rowid: int = input.get("rowid", None)
    interaction_token: str = input.get("interaction_token", None)

    if not rowid or not interaction_token:
        raise ValueError("missing argument")

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