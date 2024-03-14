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

app = Celery("tasks", broker=CELERY_BROKER, backend=CELERY_RESULT_BACKEND)

DATABASE_PATH = os.getenv("DATABASE_PATH")
S3_BUCKET = os.getenv("S3_BUCKET")

DISCORD_BOT_APPLICATION_ID = os.getenv("DISCORD_BOT_APPLICATION_ID")
AUTHORIZATION = os.getenv("AUTHORIZATION")


# https://stackoverflow.com/a/26546788
# wraps Celery Tasks such that if a single Dict is passed as args input,
# the Dict's keypairs are moved to the kwargs of the argument
# this allows the Task to have a more precise signature than just "input: Dict"
class KWArgsTask(Task):
    abstract = True    

    # args comes from the previous Task
    # kwargs comes from any arguments that are set
    def __call__(self, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], dict):
            kwargs.update(args[0])
            args = ()
        return super(KWArgsTask, self).__call__(*args, **kwargs)


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

    return rowid
    

@app.task
def download_image_and_upload_to_s3(source_url: str) -> Tuple[str, str, str, str]:
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

    return file_name, content_type, encoded_image, s3_url

# combines results from a Celery group into a Dict to passed to future Tasks as a single Dict
@app.task
def gather_results(results: List[Any], *args, **kwargs) -> Dict:
    if len(results) != len(args):
        raise ValueError("args has incorrect number of key names.")
    
    if any(not isinstance(arg, str) for arg in args):
        raise ValueError("args expects only string arguments.")
    
    return dict(zip(args, results)).update(kwargs)


@app.task(base=KWArgsTask)
def update_database_with_image(image: Tuple[str, str, str, str], rowid: int):
    _, _, _, new_url = image

    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()

    update_death_image_url_db(cursor, rowid, new_url)
    conn.commit()
    conn.close()


# update_interaction_with_image is chained from download_image_and_upload_to_s3,
# so file_name, image_content and new_url has to be first
@app.task(base=KWArgsTask)
def update_interaction_with_image(image: Tuple[str, str, str, str], interaction_token: str):
    file_name, file_content_type, image_content, _ = image

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


@app.task(base=KWArgsTask)
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