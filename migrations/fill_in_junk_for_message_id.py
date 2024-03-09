import uuid
import sqlite3
import os

DATABASE_PATH = os.getenv("DATABASE_PATH")

def connect_to_database() -> sqlite3.Connection:
    return sqlite3.connect(DATABASE_PATH)

def migrate():
    connection = connect_to_database()
    readCursor = connection.cursor()
    writeCursor = connection.cursor()

    readCursor.execute("SELECT rowid, message_id FROM deaths")
    for row in readCursor:
        rowid, message_id = row

        print (rowid, message_id)

        if message_id != "":
            continue

        new_message_id = f"GARBAGE{uuid.uuid4()}"
        writeCursor.execute(
            "UPDATE deaths SET message_id = :message_id WHERE rowid = :rowid",
            { "rowid": rowid, "message_id": new_message_id }
        )
        connection.commit()

    connection.close()

if __name__ == "__main__":
    migrate()