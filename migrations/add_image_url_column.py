import json as python_json
import sqlite3
import os

DATABASE_PATH = os.getenv("DATABASE_PATH")

def connect_to_database() -> sqlite3.Connection:
    return sqlite3.connect(DATABASE_PATH)

def migrate():
    connection = connect_to_database()
    readCursor = connection.cursor()
    writeCursor = connection.cursor()

    readCursor.execute("SELECT rowid, attachment FROM deaths")
    for row in readCursor:
        rowid, attachmentJson = row
        attachment = python_json.loads(attachmentJson)
        image_url = attachment["url"]

        print (rowid, image_url)
        writeCursor.execute(
            "UPDATE deaths SET image_url = :image_url WHERE rowid = :rowid",
            { "rowid": rowid, "image_url": image_url }
        )
        connection.commit()

    connection.close()

if __name__ == "__main__":
    migrate()