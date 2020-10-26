import sys
import os
import time
import traceback

from datetime import datetime, date

import psycopg2
import psycopg2.extras

from pyspark.sql import SparkSession


HOST = os.environ.get("POSTGRES_HOST", "db")
USER = os.environ.get("POSTGRES_USER", "vacancy")
PASSWORD = os.environ.get("POSTGRES_PASSWORD", "psql")
DB = os.environ.get("POSTGRES_DB", "vacancy")

PARQUET_FILE = "/vacancy.parquet"
ROWS_PER_FILE = 50000

RECHECK_EVERY_SEC = 60

def log(*args, file=sys.stderr, **kwargs):
    timestamp = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    print(timestamp, *args, **kwargs, file=file, flush=True)


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


def get_parquet_max_date(spark):
    DEFAULT_DATE = date(year=1970, month=1, day=1)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

    SUCCESS_FILE = f"{PARQUET_FILE}/_SUCCESS"

    try:
        file = spark._jvm.org.apache.hadoop.fs.Path(SUCCESS_FILE)
        time_ts = fs.getFileStatus(file).getModificationTime() / 1000
        return date.fromtimestamp(time_ts)
    except Exception:
        log("Exception while trying to get parquet max date")
        log(traceback.format_exc())
        return DEFAULT_DATE

def run_once():
    conn = psycopg2.connect(dbname=DB, user=USER, password=PASSWORD, host=HOST)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    max_date_so_far = get_db_max_date(cursor)

    cursor.close()
    conn.close()

    log(f"Checking last modtime in spark")
    spark = SparkSession.builder.master('local').config("spark.executor.memory", "4g").config("spark.driver.memory", "4g").getOrCreate()
    parquet_date = get_parquet_max_date(spark)

    log(f"Parquet date {parquet_date}, db date {max_date_so_far}")

    if parquet_date >= max_date_so_far:
        if parquet_date > max_date_so_far:
            log(f"Parquet date is from future")
        return

    properties = {
        "driver": "org.postgresql.Driver",
        "user": USER,
        "password": PASSWORD
    }

    log(f"Saving db to hdfs://{PARQUET_FILE}")
    df = spark.read.jdbc(url=f"jdbc:postgresql://{HOST}/{DB}",table='vacancy',properties=properties)
    df.write.option("maxRecordsPerFile", ROWS_PER_FILE).parquet(PARQUET_FILE, mode="overwrite")
    log(f"Parquet saved")

def loop():
    log(f"Starting the hadoop feeder loop")

    while True:
        try:
            run_once()
        except Exception:
            log(traceback.format_exc())
        time.sleep(RECHECK_EVERY_SEC)


if __name__ == "__main__":
    loop()
