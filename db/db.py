import sqlite3
from numbers import Number
import secrets
from typing import Dict, List, Tuple

INSERT_DEATH_SQL = """INSERT INTO deaths VALUES (:server, :dead_person, :caption, :attachment, :image_url, :timestamp, :reporter, :interaction_id)"""
SELECT_DEADPERSON_COUNT_SQL = """SELECT dead_person, COUNT(rowid) FROM deaths GROUP BY dead_person"""
SELECT_DEADPERSON_SQL = """SELECT caption, attachment, timestamp, reporter FROM deaths WHERE dead_person = :dead_person"""
DELETE_DEATH_SQL = """DELETE FROM deaths WHERE interaction_id = :interaction_id"""
UPDATE_DEATH_IMAGE_URL_SQL = """UPDATE deaths SET image_url = :image_url WHERE rowid = :rowid"""


def connect_to_database(path: str) -> sqlite3.Connection:
    return sqlite3.connect(path)


def add_death_db(
    cursor: sqlite3.Cursor,
    server: str,
    dead_person: str,
    caption: str,
    attachment: any,
    image_url: str,
    timestamp: Number,
    reporter: str,
    interaction_id: str,
) -> int:
    cursor.execute(
        INSERT_DEATH_SQL,
        {
            "server": server,
            "dead_person": dead_person,
            "caption": caption,
            "attachment": attachment,
            "image_url": image_url,
            "timestamp": timestamp,
            "reporter": reporter,
            "interaction_id": interaction_id,
        },
    )

    return cursor.lastrowid


def get_tally_db(cursor: sqlite3.Cursor) -> List[Tuple[str, int]]:
    response = cursor.execute(SELECT_DEADPERSON_COUNT_SQL)
    return response.fetchall()


def get_death_db(cursor: sqlite3.Cursor, dead_person: str) -> Dict:
    response = cursor.execute(SELECT_DEADPERSON_SQL, { "dead_person": dead_person })
    result = secrets.choice(response.fetchall())
    return {
        "caption": result[0],
        "attachment": result[1],
        "timestamp": result[2],
        "reporter": result[3],
    }


def delete_death_db(cursor: sqlite3.Cursor, interaction_id: str):
    cursor.execute(DELETE_DEATH_SQL, { "interaction_id": interaction_id })


def update_death_image_url_db(cursor: sqlite3.Cursor, rowid: int, image_url: str):
    cursor.execute(UPDATE_DEATH_IMAGE_URL_SQL, { "rowid": rowid, "image_url": image_url })