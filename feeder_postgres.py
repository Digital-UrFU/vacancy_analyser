import sys
import os
import csv
import re
import time
import traceback

from datetime import datetime, date

import psycopg2
import psycopg2.extras
import dotenv

from psycopg2.extensions import AsIs

try:
    dotenv.load_dotenv("postgres.env")
except OSError:
    pass

HOST = os.environ.get("POSTGRES_HOST", "db")
USER = os.environ.get("POSTGRES_USER", "vacancy")
PASSWORD = os.environ.get("POSTGRES_PASSWORD", "psql")
DB = os.environ.get("POSTGRES_DB", "vacancy")

DATA_DIR = "data"

RECHECK_EVERY_SEC = 60

def log(*args, file=sys.stderr, **kwargs):
    timestamp = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    print(timestamp, *args, **kwargs, file=file, flush=True)


def create_vacancy_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vacancy (
            id BIGINT PRIMARY KEY NOT NULL,
            description TEXT,
            key_skills TEXT,
            schedule_id VARCHAR(1024),
            schedule_name VARCHAR(1024),
            accept_handicapped BOOLEAN,
            accept_kids BOOLEAN,
            experience_id VARCHAR(1024),
            experience_name VARCHAR(1024),
            specializations TEXT,
            contacts TEXT,
            billing_type_id VARCHAR(1024),
            billing_type_name VARCHAR(1024),
            allow_messages BOOLEAN,
            premium BOOLEAN,
            driver_license_types TEXT,
            accept_incomplete_resumes BOOLEAN,
            employer_id BIGINT,
            employer_name TEXT,
            employer_vacancies_url TEXT,
            employer_trusted BOOLEAN,
            employer_alternate_url TEXT,
            employer_industries TEXT,
            response_letter_required BOOLEAN,
            type_id VARCHAR(1024),
            type_name VARCHAR(1024),
            has_test BOOLEAN,
            response_url TEXT,
            test_required BOOLEAN,
            salary_from BIGINT,
            salary_to BIGINT,
            salary_gross BOOLEAN,
            salary_currency VARCHAR(64),
            archived BOOLEAN,
            name TEXT,
            insider_interview TEXT,
            area_id INT,
            area_name VARCHAR(1024),
            area_url TEXT,
            created_at TIMESTAMP,
            published_at TIMESTAMP,
            address_city VARCHAR(1024),
            address_street VARCHAR(1024),
            address_building VARCHAR(1024),
            address_description TEXT,
            address_lat DOUBLE PRECISION,
            address_lng DOUBLE PRECISION,
            alternate_url TEXT,
            apply_alternate_url TEXT,
            code TEXT,
            department_id VARCHAR(1024),
            department_name VARCHAR(1024),
            employment_id VARCHAR(1024),
            employment_name VARCHAR(1024),
            added_at DATE,
            updated_at DATE,
            removed_at DATE
        )
    """)

    cursor.execute("CREATE INDEX ON vacancy (area_id)")
    cursor.execute("CREATE INDEX ON vacancy (area_name)")
    cursor.execute("CREATE INDEX ON vacancy (added_at)")
    cursor.execute("CREATE INDEX ON vacancy (updated_at)")
    cursor.execute("CREATE INDEX ON vacancy (removed_at)")
    cursor.execute("CREATE INDEX ON vacancy (archived)")

def cut_text(text, limit=128):
    text = str(text)
    if len(text) < limit:
        return text
    return text[:limit] + "..."

def feed_csv(csv_reader, csv_date, cursor, logfile):
    STATS_EVERY = 1000
    known_ids = set()

    items_added = 0
    items_updated = 0
    items_removed = 0

    for pos, csv_row in enumerate(csv_reader):
        if pos % STATS_EVERY == 0:
            log(f"Rows feeded={pos} added={items_added} updated={items_updated} removed={items_removed}")

        csv_row["id"] = int(csv_row["id"])
        csv_row["created_at"] = datetime.fromisoformat(csv_row["created_at"].split("+")[0])
        csv_row["published_at"] = datetime.fromisoformat(csv_row["published_at"].split("+")[0])

        for k in csv_row:
            if not csv_row[k]:
                csv_row[k] = None

        csv_row["archived"] = (csv_row["archived"].lower() == "true")
        if csv_row["archived"]:
            # consider archived vacations as deleted ones
            continue

        known_ids.add(csv_row["id"])

        cursor.execute("SELECT * FROM vacancy WHERE id=%s", (csv_row["id"], ))
        db_row = cursor.fetchone()

        if not db_row:
            log(f"Row {csv_row['id']}: adding new record", file=logfile)

            csv_row["added_at"] = csv_date
            csv_row["updated_at"] = csv_row["added_at"]

            columns = tuple(k for k in csv_row if csv_row[k])
            values = tuple(csv_row[k] for k in columns)

            cursor.execute("INSERT INTO vacancy (%s) values %s", (AsIs(','.join(columns)), values))
            items_added += 1
            continue

        csv_row["added_at"] = min(csv_date, db_row.get("added_at", csv_date))

        if not db_row["updated_at"] or db_row["updated_at"] > csv_date:
            log(f"Row {csv_row['id']}: newer record detected, firing error just in case")
            log(f"Row {csv_row['id']}: newer record detected, firing error just in case", file=logfile)
            raise Exception("newer record detected")

        # find the differences
        columns = tuple(k for k in csv_row if str(csv_row[k]) != str(db_row.get(k)))
        values = tuple(csv_row[k] for k in columns)

        was_update = False

        for column, value in zip(columns, values):
            log(f"Row {csv_row['id']}: updating column {column} from " +
                f"{cut_text(db_row.get(column))} to {cut_text(value)}", file=logfile)

            cursor.execute("UPDATE vacancy SET %s = %s WHERE id = %s ", (AsIs(column), value, csv_row["id"]))

            if column != "added_at":
                was_update = True

        if was_update:
            cursor.execute("UPDATE vacancy SET updated_at = %s WHERE id = %s ", (csv_date, csv_row["id"]))
            items_updated += 1

    # mark disapeared records as removed
    cursor.execute("SELECT id, removed_at FROM vacancy WHERE added_at < %s", (csv_date,))

    to_remove = []
    for row in cursor:
        if row["id"] not in known_ids:
            if not row["removed_at"] or csv_date < row["removed_at"]:
                to_remove.append(row["id"])

    for row_id in to_remove:
        log(f"Row {row_id}: marking as removed at {csv_date}", file=logfile)
        cursor.execute("UPDATE vacancy SET removed_at = %s WHERE id = %s", (csv_date, row_id))
        items_removed += 1

    log(f"Items: added={items_added}, updated={items_updated}, removed={items_removed}")


def get_db_max_date(cursor):
    DEFAULT_DATE = date(year=1970, month=1, day=1)

    cursor.execute("select max(added_at),max(updated_at),max(removed_at) from vacancy;")

    row = cursor.fetchone()
    if not row:
        return DEFAULT_DATE

    dates = [d for d in row if d]
    if not dates:
        return DEFAULT_DATE
    return max(dates)


def run_once():
    DATE_RE = r"\d\d\d\d-\d\d-\d\d"
    CSV_FILENAME = "result.csv"
    LOG_FILENAME = "feeder_postgres_log.txt"

    log(f"Checking dirs to feed")

    conn = psycopg2.connect(dbname=DB, user=USER, password=PASSWORD, host=HOST)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    create_vacancy_table(cursor)

    max_date_so_far = get_db_max_date(cursor)

    dirs = sorted(d for d in os.listdir() if re.fullmatch(DATE_RE, d, re.ASCII))

    for curr_dir in dirs:
        csv_dir_date = datetime.strptime(curr_dir, "%Y-%m-%d").date()
        if csv_dir_date <= max_date_so_far:
            continue

        csv_filename = os.path.join(curr_dir, CSV_FILENAME)
        log_filename = os.path.join(curr_dir, LOG_FILENAME)
        log_filename_pretty = os.path.join(DATA_DIR, log_filename)

        log(f"Feeding dir {curr_dir}, log file {log_filename_pretty}")

        with open(log_filename, "w", encoding="utf8") as logfile:
            with open(csv_filename, newline="", encoding="utf8") as csv_file:
                csv_reader = csv.DictReader(csv_file)
                feed_csv(csv_reader, csv_dir_date, cursor, logfile)

        conn.commit()
        log(f"Finished feeding dir {curr_dir}")

    cursor.close()
    conn.close()


def loop():
    log(f"Starting the feeder loop")

    while True:
        try:
            run_once()
        except Exception:
            log(traceback.format_exc())
        time.sleep(RECHECK_EVERY_SEC)


if __name__ == "__main__":
    os.chdir(DATA_DIR)
    loop()
