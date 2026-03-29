"""Wait for PostgreSQL to become available before starting the app."""

import os
import sys
import time

import psycopg2


def wait_for_db(max_retries: int = 30, delay: float = 2.0) -> None:
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("DATABASE_URL not set, skipping DB wait")
        return

    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(db_url)
            conn.close()
            print(f"Database ready (attempt {attempt})")
            return
        except psycopg2.OperationalError:
            print(f"Waiting for database... ({attempt}/{max_retries})")
            time.sleep(delay)

    print("Could not connect to database")
    sys.exit(1)


if __name__ == "__main__":
    wait_for_db()
