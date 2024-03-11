import sqlite3
from numbers import Number
import secrets
from typing import Dict, List, Tuple

INSERT_DEATH_SQL = """INSERT INTO deaths VALUES (:server, :channel_id, :message_id, :dead_person, :caption, :attachment, :image_url, :timestamp, :reporter)"""
SELECT_DEADPERSON_COUNT_SQL = """SELECT dead_person, COUNT(rowid) FROM deaths WHERE server = :guild_id GROUP BY dead_person"""
SELECT_DEADPERSON_COUNT_BY_TIME_SQL = """SELECT dead_person, COUNT(rowid) FROM deaths WHERE timestamp BETWEEN :start_time AND :end_time AND server = :guild_id GROUP BY dead_person"""
SELECT_DEADPERSON_SQL = """SELECT caption, attachment, timestamp, reporter FROM deaths WHERE server = :guild_id AND dead_person = :dead_person"""
SELECT_DEADPERSON_BY_MESSAGE_ID = """SELECT rowid, server, channel_id, dead_person, caption, reporter FROM deaths WHERE message_id = :message_id"""
UPDATE_DEATH_IMAGE_URL_SQL = """UPDATE deaths SET image_url = :image_url WHERE rowid = :rowid"""
UPDATE_DEATH_MESSAGE_ID_SQL = """UPDATE deaths SET message_id = :message_id WHERE rowid = :rowid"""
DELETE_BY_ROWID_SQL = """DELETE FROM deaths WHERE rowid = :rowid"""


def connect_to_database(path: str) -> sqlite3.Connection:
    return sqlite3.connect(path)


def add_death_db(
    cursor: sqlite3.Cursor,
    server: str,
    channel_id: str,
    message_id: str,
    dead_person: str,
    caption: str,
    attachment: any,
    image_url: str,
    timestamp: Number,
    reporter: str,
) -> int:
    cursor.execute(
        INSERT_DEATH_SQL,
        {
            "server": server,
            "channel_id": channel_id,
            "message_id": message_id,
            "dead_person": dead_person,
            "caption": caption,
            "attachment": attachment,
            "image_url": image_url,
            "timestamp": timestamp,
            "reporter": reporter,
        },
    )

    return cursor.lastrowid


def get_tally_db(cursor: sqlite3.Cursor, guild_id: str) -> List[Tuple[str, int]]:
    response = cursor.execute(SELECT_DEADPERSON_COUNT_SQL, {
        "guild_id": guild_id,
    })
    return response.fetchall()


def get_tally_time_db(cursor: sqlite3.Cursor, guild_id: str, start_time: int, end_time: int) -> List[Tuple[str, int]]:
    response = cursor.execute(SELECT_DEADPERSON_COUNT_BY_TIME_SQL, {
        "start_time": start_time,
        "end_time": end_time,
        "guild_id": guild_id,
    })
    return response.fetchall()

def get_death_db(cursor: sqlite3.Cursor, guild_id: str, dead_person: str) -> Dict:
    response = cursor.execute(SELECT_DEADPERSON_SQL, {
        "guild_id": guild_id,
        "dead_person": dead_person,
    })
    result = secrets.choice(response.fetchall())
    return {
        "caption": result[0],
        "attachment": result[1],
        "timestamp": result[2],
        "reporter": result[3],
    }


def get_death_by_message_id_db(cursor: sqlite3.Cursor, message_id: str) -> Tuple:
    response = cursor.execute(SELECT_DEADPERSON_BY_MESSAGE_ID, { "message_id": message_id })
    return response.fetchone() # a message ID should only correspond to one death (fingers crossed)


def update_death_image_url_db(cursor: sqlite3.Cursor, rowid: int, image_url: str):
    cursor.execute(UPDATE_DEATH_IMAGE_URL_SQL, { "rowid": rowid, "image_url": image_url })


def update_death_message_id_db(cursor: sqlite3.Cursor, rowid: int, message_id: str):
    cursor.execute(UPDATE_DEATH_MESSAGE_ID_SQL, { "rowid": rowid, "message_id": message_id })


def delete_death_db(cursor: sqlite3.Cursor, rowid: int):
    cursor.execute(DELETE_BY_ROWID_SQL, { "rowid": rowid })